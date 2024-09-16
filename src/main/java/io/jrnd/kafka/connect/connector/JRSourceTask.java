// Copyright © 2024 JR team
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package io.jrnd.kafka.connect.connector;

import io.jrnd.kafka.connect.connector.format.avro.AvroHelper;
import io.jrnd.kafka.connect.connector.format.StructHelper;
import io.jrnd.kafka.connect.connector.format.jsonschema.JsonSchemaHelper;
import io.jrnd.kafka.connect.connector.format.protobuf.ProtobufHelper;
import io.jrnd.kafka.connect.connector.model.Template;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.storage.StringConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class JRSourceTask extends SourceTask {

    private String template;
    private String embeddedTemplate;
    private String topic;
    private Long pollMs;
    private Integer objects;
    private String keyField;
    private Integer keyValueIntervalMax;
    private Long last_execution = 0L;
    private Long apiOffset = 0L;
    private String fromDate = "1970-01-01T00:00:00.0000000Z";
    private String jrExecutablePath;
    private String valueConverter;

    private static final String TEMPLATE = "template";
    private static final String POSITION = "position";

    private static final Logger LOG = LoggerFactory.getLogger(JRSourceTask.class);

    @Override
    public String version() {
        return null;
    }

    @Override
    public void start(Map<String, String> map) {
        if(map.containsKey(JRSourceConnector.JR_EXISTING_TEMPLATE))
            template = map.get(JRSourceConnector.JR_EXISTING_TEMPLATE);
        topic = map.get(JRSourceConnector.TOPIC_CONFIG);
        pollMs = Long.valueOf(map.get(JRSourceConnector.POLL_CONFIG));
        objects = Integer.valueOf(map.get(JRSourceConnector.OBJECTS_CONFIG));
        if(map.containsKey(JRSourceConnector.EMBEDDED_TEMPLATE))
            embeddedTemplate = map.get(JRSourceConnector.EMBEDDED_TEMPLATE);
        if(map.containsKey(JRSourceConnector.KEY_FIELD))
            keyField = map.get(JRSourceConnector.KEY_FIELD);
        if(map.containsKey(JRSourceConnector.KEY_VALUE_INTERVAL_MAX))
            keyValueIntervalMax = Integer.valueOf(map.get(JRSourceConnector.KEY_VALUE_INTERVAL_MAX));
        jrExecutablePath = map.get(JRSourceConnector.JR_EXECUTABLE_PATH);
        valueConverter = map.get(JRSourceConnector.VALUE_CONVERTER);

        Map<String, Object> offset = context.offsetStorageReader().offset(Collections.singletonMap(TEMPLATE, template));
        if (offset != null) {
            Long lastRecordedOffset = (Long) offset.get(POSITION);
            if (lastRecordedOffset != null) {
                if (LOG.isDebugEnabled())
                    LOG.debug("Loaded offset: {}", apiOffset);
                apiOffset = lastRecordedOffset;
            }
        }
    }

    @Override
    public List<SourceRecord> poll() {

        if (System.currentTimeMillis() > (last_execution + pollMs)) {

            if (LOG.isDebugEnabled()) {
                LOG.debug("init fromDate is {}.", fromDate);
                LOG.debug("Generate records for template: {}", template);
            }

            last_execution = System.currentTimeMillis();
            JRCommandExecutor jrCommandExecutor = JRCommandExecutor.getInstance(jrExecutablePath);
            Template templateWrapper = new Template();
            templateWrapper.setTemplate(template);
            if(embeddedTemplate != null && !embeddedTemplate.isEmpty()) {
                templateWrapper.setEmbedded(true);
                templateWrapper.setTemplate(embeddedTemplate);
            }
            List<String> result = jrCommandExecutor.runTemplate(templateWrapper, objects, keyField, keyValueIntervalMax);

            if (LOG.isDebugEnabled())
                LOG.debug("Result from JR command: {}", result);

            List<SourceRecord> sourceRecords = new ArrayList<>();

            int index = 1;
            String key = null;
            for(String record: result) {

                if(keyField == null || keyField.isEmpty()) {
                    sourceRecords.add(createSourceRecord(null, record));
                } else {
                    if(index % 2 == 0) {
                        String replacement = extractReplacement(key);
                        String updatedRecord = replaceWithKey(keyField.toLowerCase(), record, replacement);
                        sourceRecords.add(createSourceRecord(key, updatedRecord));
                    } else {
                        key = record;
                    }
                    index++;
                }

                if (LOG.isDebugEnabled())
                    LOG.debug("new fromDate is {}.", fromDate);
            }

            return sourceRecords;
        }
        return Collections.emptyList();
    }

    @Override
    public void stop() {}

    public SourceRecord createSourceRecord(String recordKey, String recordValue) {
        String newFromDate = LocalDateTime.now().atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        apiOffset = calculateApiOffset(apiOffset, newFromDate, fromDate);
        fromDate = newFromDate;

        Map<String, Object> sourcePartition = Collections.singletonMap(TEMPLATE, template);
        Map<String, Long> sourceOffset = Collections.singletonMap(POSITION, ++apiOffset);

        String messageName = template;
        if(embeddedTemplate != null && !embeddedTemplate.isEmpty()) {
            messageName = "record";
        }

        if(valueConverter.equals(StringConverter.class.getName())) {
            if (recordKey != null && !recordKey.isEmpty())
                return new SourceRecord(sourcePartition, sourceOffset, topic, Schema.STRING_SCHEMA, recordKey, Schema.STRING_SCHEMA, recordValue);
            else
                return new SourceRecord(sourcePartition, sourceOffset, topic, Schema.STRING_SCHEMA, recordValue);
        }
        //FIXME eliminate static string
        else if(valueConverter.equals("io.confluent.connect.protobuf.ProtobufConverter")) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Protobuf Schema output format required");
            }

            try {
                Schema kafkaConnectSchema =  ProtobufHelper.createProtobufSchemaFromJson(messageName, recordValue);
                return createSourceRecordWithSchema(recordKey, recordValue, kafkaConnectSchema, sourcePartition, sourceOffset);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        else if(valueConverter.equals("io.confluent.connect.json.JsonSchemaConverter")) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Json Schema output format required");
            }

            try {
                Schema kafkaConnectSchema =  JsonSchemaHelper.createJsonSchemaFromJson(recordValue);
                return createSourceRecordWithSchema(recordKey, recordValue, kafkaConnectSchema, sourcePartition, sourceOffset);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        else if(valueConverter.equals("io.confluent.connect.avro.AvroConverter")){
            if (LOG.isDebugEnabled()) {
                LOG.debug("Avro output format required");
            }

            try {
                org.apache.avro.Schema schema =  AvroHelper.createAvroSchemaFromJson(messageName + "Record", recordValue);
                Schema kafkaConnectSchema = AvroHelper.convertAvroToConnectSchema(schema);

                return createSourceRecordWithSchema(recordKey, recordValue, kafkaConnectSchema, sourcePartition, sourceOffset);

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        else {
            if (LOG.isErrorEnabled()) {
                LOG.error("Converter class not supported");
                throw new RuntimeException();
            }
        }
        return null;
    }

    public long calculateApiOffset(long currentLoopOffset, String newFromDate, String oldFromDate) {
        if (newFromDate.equals(oldFromDate)) {
            return ++currentLoopOffset;
        }
        return 1L;
    }

    private SourceRecord createSourceRecordWithSchema(String recordKey, String recordValue, Schema kafkaConnectSchema, Map<String, Object> sourcePartition, Map<String, Long> sourceOffset) throws IOException {
        Struct structValue = StructHelper.convertJsonToStruct(kafkaConnectSchema, recordValue);

        if (recordKey != null && !recordKey.isEmpty())
            return new SourceRecord(sourcePartition, sourceOffset, topic, Schema.STRING_SCHEMA, recordKey, kafkaConnectSchema, structValue);
        else
            return new SourceRecord(sourcePartition, sourceOffset, topic, kafkaConnectSchema, structValue);
    }

    private static String extractReplacement(String json) {
        return json.substring(1, json.length() - 1);
    }

    private static String replaceWithKey(String keyToMatch, String originalJson, String replacement) {
        String regex = "\""+keyToMatch+"\":\\s*\"[^\"]*\"";
        return originalJson.replaceAll(regex, replacement);
    }

    public String getTemplate() {
        return template;
    }

    public String getTopic() {
        return topic;
    }

    public Long getPollMs() {
        return pollMs;
    }

    public Integer getObjects() {
        return objects;
    }

    public void setLast_execution(Long last_execution) {
        this.last_execution = last_execution;
    }

    public Long getApiOffset() {
        return apiOffset;
    }

    public String getKeyField() {
        return keyField;
    }

    public Integer getKeyValueIntervalMax() {
        return keyValueIntervalMax;
    }

}

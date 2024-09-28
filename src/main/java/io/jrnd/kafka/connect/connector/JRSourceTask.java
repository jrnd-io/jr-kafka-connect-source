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
    private Long startTimeMs;
    private Long finalTimeMs;
    private int pollIteration = 0;
    private Integer objects;
    private String keyField;
    private Integer keyValueIntervalMax;
    private String keyEmbeddedTemplate;
    private Long last_execution = 0L;
    private Long apiOffset = 0L;
    private String fromDate = "1970-01-01T00:00:00.0000000Z";
    private String jrExecutablePath;
    private String valueConverter;
    private String keyConverter;

    private static final String TEMPLATE = "template";
    private static final String POSITION = "position";

    private final static String AVRO_CONVERTER_CLASS_NAME = "io.confluent.connect.avro.AvroConverter";
    private final static String JSON_SCHEMA_CONVERTER_CLASS_NAME = "io.confluent.connect.json.JsonSchemaConverter";
    private final static String PROTOBUF_CONVERTER_CLASS_NAME = "io.confluent.connect.protobuf.ProtobufConverter";

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
        if(map.containsKey(JRSourceConnector.DURATION_CONFIG)) {
            long durationMs = Long.parseLong(map.get(JRSourceConnector.DURATION_CONFIG));
            if(durationMs > 1) {
                startTimeMs = System.currentTimeMillis();
                finalTimeMs = startTimeMs + durationMs;
            }
        }
        objects = Integer.valueOf(map.get(JRSourceConnector.OBJECTS_CONFIG));
        if(map.containsKey(JRSourceConnector.EMBEDDED_TEMPLATE))
            embeddedTemplate = map.get(JRSourceConnector.EMBEDDED_TEMPLATE);
        if(map.containsKey(JRSourceConnector.KEY_FIELD))
            keyField = map.get(JRSourceConnector.KEY_FIELD);
        if(map.containsKey(JRSourceConnector.KEY_VALUE_INTERVAL_MAX))
            keyValueIntervalMax = Integer.valueOf(map.get(JRSourceConnector.KEY_VALUE_INTERVAL_MAX));
        if(map.containsKey(JRSourceConnector.KEY_EMBEDDED_TEMPLATE))
            keyEmbeddedTemplate = map.get(JRSourceConnector.KEY_EMBEDDED_TEMPLATE);
        jrExecutablePath = map.get(JRSourceConnector.JR_EXECUTABLE_PATH);
        valueConverter = map.get(JRSourceConnector.VALUE_CONVERTER);
        keyConverter = map.get(JRSourceConnector.KEY_CONVERTER);

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

        long currentTime = System.currentTimeMillis();
        if (currentTime > (last_execution + pollMs)) {

            if(pollIteration == 0 || startTimeMs == null || currentTime < finalTimeMs) {

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Generate records for template {} - currentTime {} - finalTime {}", template, currentTime, finalTimeMs);
                }

                last_execution = System.currentTimeMillis();
                pollIteration = pollIteration + 1;

                // Dispatch run template command to JR exec
                JRCommandExecutor jrCommandExecutor = JRCommandExecutor.getInstance(jrExecutablePath);
                Template templateWrapper = getTemplateWrapper();

                // Process results from JR exec
                List<String> result = jrCommandExecutor.runTemplate(templateWrapper, objects, keyField, keyValueIntervalMax);

                if (LOG.isDebugEnabled())
                    LOG.debug("Result from JR exec: {}", result);

                // Create Kafka Connect Source Records
                List<SourceRecord> sourceRecords = new ArrayList<>();
                int index = 1;
                String key = null;
                for (String record : result) {
                    if ( (keyField == null || keyField.isEmpty()) && (keyEmbeddedTemplate == null || keyEmbeddedTemplate.isEmpty())) {
                        sourceRecords.add(createSourceRecord(null, record));
                    } else {
                        if (index % 2 == 0) {
                            //TODO replacement for key embedded
                            if(keyEmbeddedTemplate == null || keyEmbeddedTemplate.isEmpty()) {
                                String replacement = extractReplacement(key);
                                String updatedRecord = replaceWithKey(keyField.toLowerCase(), record, replacement);
                                sourceRecords.add(createSourceRecord(key, updatedRecord));
                            } else {
                                sourceRecords.add(createSourceRecord(key, record));
                            }
                        } else {
                            key = record;
                        }
                        index++;
                    }
                }

                return sourceRecords;
            }
        }
        return Collections.emptyList();
    }

    private Template getTemplateWrapper() {
        Template templateWrapper = new Template();
        templateWrapper.setTemplate(template);
        if (embeddedTemplate != null && !embeddedTemplate.isEmpty()) {
            templateWrapper.setEmbedded(true);
            templateWrapper.setTemplate(embeddedTemplate);
        }
        if (keyEmbeddedTemplate != null && !keyEmbeddedTemplate.isEmpty()) {
            templateWrapper.setKeyEmbedded(true);
            templateWrapper.setKeyTemplate(keyEmbeddedTemplate);
        }
        return templateWrapper;
    }

    @Override
    public void stop() {}

    public SourceRecord createSourceRecord(String recordKey, String recordValue) {
        String newFromDate = LocalDateTime.now().atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        apiOffset = calculateApiOffset(apiOffset, newFromDate, fromDate);
        fromDate = newFromDate;

        Map<String, Object> sourcePartition = Collections.singletonMap(TEMPLATE, template);
        Map<String, Long> sourceOffset = Collections.singletonMap(POSITION, ++apiOffset);

        String valueSchemaName = template;
        String keySchemaName = "record-key";
        if(embeddedTemplate != null && !embeddedTemplate.isEmpty()) {
            valueSchemaName = "record-value";
        }

        // Case: no schema required for key
        if ( (keyEmbeddedTemplate == null || keyEmbeddedTemplate.isEmpty()) || keyConverter.equals(StringConverter.class.getName())) {
            Schema valueKafkaConnectSchema;
            if (valueConverter.equals(StringConverter.class.getName())) {
                if (recordKey != null && !recordKey.isEmpty())
                    return new SourceRecord(sourcePartition, sourceOffset, topic, Schema.STRING_SCHEMA, recordKey, Schema.STRING_SCHEMA, recordValue);
                else
                    return new SourceRecord(sourcePartition, sourceOffset, topic, Schema.STRING_SCHEMA, recordValue);
            } else if (valueConverter.equals(PROTOBUF_CONVERTER_CLASS_NAME)) {
                try {
                    valueKafkaConnectSchema = ProtobufHelper.createProtobufSchemaFromJson(valueSchemaName, recordValue);
                    return createSourceRecordWithSchema(recordKey, recordValue, Schema.STRING_SCHEMA, valueKafkaConnectSchema, sourcePartition, sourceOffset);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else if (valueConverter.equals(JSON_SCHEMA_CONVERTER_CLASS_NAME)) {
                try {
                    valueKafkaConnectSchema = JsonSchemaHelper.createJsonSchemaFromJson(recordValue);
                    return createSourceRecordWithSchema(recordKey, recordValue, Schema.STRING_SCHEMA, valueKafkaConnectSchema, sourcePartition, sourceOffset);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else if (valueConverter.equals(AVRO_CONVERTER_CLASS_NAME)) {
                try {
                    org.apache.avro.Schema schema = AvroHelper.createAvroSchemaFromJson(valueSchemaName + "Record", recordValue);
                    valueKafkaConnectSchema = AvroHelper.convertAvroToConnectSchema(schema);
                    return createSourceRecordWithSchema(recordKey, recordValue, Schema.STRING_SCHEMA, valueKafkaConnectSchema, sourcePartition, sourceOffset);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                handleConverterNotSupportedException();
            }
        }
        // Case: key schema required
        else {
            Schema keyKafkaConnectSchema;
            Schema valueKafkaConnectSchema = Schema.STRING_SCHEMA;
            switch (keyConverter) {
                case PROTOBUF_CONVERTER_CLASS_NAME -> {
                    try {
                        keyKafkaConnectSchema = ProtobufHelper.createProtobufSchemaFromJson(keySchemaName, recordKey);
                        if (valueConverter.equals(PROTOBUF_CONVERTER_CLASS_NAME))
                            valueKafkaConnectSchema = ProtobufHelper.createProtobufSchemaFromJson(valueSchemaName, recordValue);
                        return createSourceRecordWithSchema(recordKey, recordValue, keyKafkaConnectSchema, valueKafkaConnectSchema, sourcePartition, sourceOffset);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                case JSON_SCHEMA_CONVERTER_CLASS_NAME -> {
                    try {
                        keyKafkaConnectSchema = JsonSchemaHelper.createJsonSchemaFromJson(recordKey);
                        if (valueConverter.equals(JSON_SCHEMA_CONVERTER_CLASS_NAME))
                            valueKafkaConnectSchema = JsonSchemaHelper.createJsonSchemaFromJson(recordValue);
                        return createSourceRecordWithSchema(recordKey, recordValue, keyKafkaConnectSchema, valueKafkaConnectSchema, sourcePartition, sourceOffset);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                case AVRO_CONVERTER_CLASS_NAME -> {
                    try {
                        org.apache.avro.Schema keySchema = AvroHelper.createAvroSchemaFromJson(keySchemaName + "Record", recordKey);
                        keyKafkaConnectSchema = AvroHelper.convertAvroToConnectSchema(keySchema);
                        if (valueConverter.equals(AVRO_CONVERTER_CLASS_NAME)) {
                            org.apache.avro.Schema valueSchema = AvroHelper.createAvroSchemaFromJson(valueSchemaName + "Record", recordValue);
                            valueKafkaConnectSchema = AvroHelper.convertAvroToConnectSchema(valueSchema);
                        }
                        return createSourceRecordWithSchema(recordKey, recordValue, keyKafkaConnectSchema, valueKafkaConnectSchema, sourcePartition, sourceOffset);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                default -> handleConverterNotSupportedException();
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

    private SourceRecord createSourceRecordWithSchema(
            String recordKey,
            String recordValue,
            Schema keyKafkaConnectSchema,
            Schema valueKafkaConnectSchema,
            Map<String, Object> sourcePartition,
            Map<String, Long> sourceOffset) throws IOException {
        Struct structValue = StructHelper.convertJsonToStruct(valueKafkaConnectSchema, recordValue);

        if (recordKey != null && !recordKey.isEmpty())
            return new SourceRecord(sourcePartition, sourceOffset, topic, keyKafkaConnectSchema, recordKey, valueKafkaConnectSchema, structValue);
        else
            return new SourceRecord(sourcePartition, sourceOffset, topic, valueKafkaConnectSchema, structValue);
    }

    private void handleConverterNotSupportedException() {
        if (LOG.isErrorEnabled()) {
            final String message = "Converter class not supported";
            LOG.error(message);
            throw new IllegalStateException(message);
        }
    }

    private String extractReplacement(String json) {
        return json.substring(1, json.length() - 1);
    }

    private String replaceWithKey(String keyToMatch, String originalJson, String replacement) {
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

}
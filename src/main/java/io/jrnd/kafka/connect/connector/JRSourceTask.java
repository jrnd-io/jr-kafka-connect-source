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

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class JRSourceTask extends SourceTask {

    private JRCommandExecutor jrCommandExecutor = JRCommandExecutor.getInstance();

    private String template;
    private String topic;
    private Long pollMs;
    private Integer objects;
    private String keyField;
    private Long last_execution = 0L;
    private Long apiOffset = 0L;
    private String fromDate = "1970-01-01T00:00:00.0000000Z";

    private static final String TEMPLATE = "template";
    private static final String POSITION = "position";

    private static final Logger LOG = LoggerFactory.getLogger(JRSourceTask.class);

    @Override
    public String version() {
        return null;
    }

    @Override
    public void start(Map<String, String> map) {
        template = map.get(JRSourceConnector.JR_EXISTING_TEMPLATE);
        topic = map.get(JRSourceConnector.TOPIC_CONFIG);
        pollMs = Long.valueOf(map.get(JRSourceConnector.POLL_CONFIG));
        objects = Integer.valueOf(map.get(JRSourceConnector.OBJECTS_CONFIG));
        if(map.containsKey(JRSourceConnector.KEY_FIELD))
            keyField = map.get(JRSourceConnector.KEY_FIELD);

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
            List<String> result = jrCommandExecutor.runTemplate(template, objects, keyField);

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
                        String updatedRecord = replaceWithKey(keyField.toLowerCase(), record, key);
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

        if(recordKey != null && !recordKey.isEmpty())
            return new SourceRecord(sourcePartition, sourceOffset, topic, Schema.STRING_SCHEMA, recordKey, Schema.STRING_SCHEMA, recordValue);
        else
            return new SourceRecord(sourcePartition, sourceOffset, topic, Schema.STRING_SCHEMA, recordValue);
    }

    public long calculateApiOffset(long currentLoopOffset, String newFromDate, String oldFromDate) {
        if (newFromDate.equals(oldFromDate)) {
            return ++currentLoopOffset;
        }
        return 1L;
    }

    public String replaceWithKey(String field, String originalJson, String newUserIdJson) {
        String regex = "\""+field+"\":\\s*\"[^\"]*\"";
        return originalJson.replaceAll(regex, "\""+field+"\": " + newUserIdJson);
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

}

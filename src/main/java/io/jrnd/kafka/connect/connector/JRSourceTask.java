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

import java.util.*;

public class JRSourceTask extends SourceTask {

    private String command;
    private String topic;
    private Long pollMs;
    private Long last_execution = 0L;
    private Long apiOffset = 0L;

    private static final String COMMAND = "net_device";

    private static final Logger LOG = LoggerFactory.getLogger(JRSourceTask.class);

    @Override
    public String version() {
        return null;
    }

    @Override
    public void start(Map<String, String> map) {
        command = map.get(JRSourceConnector.JR_EXISTING_TEMPLATE);
        topic = map.get(JRSourceConnector.TOPIC_CONFIG);
        pollMs = Long.valueOf(map.get(JRSourceConnector.POLL_CONFIG));

        Map<String, Object> offset = context.offsetStorageReader().offset(Collections.singletonMap(COMMAND, command));
        if (offset != null) {
            Long lastRecordedOffset = (Long) offset.get("position");
            if (lastRecordedOffset != null) {
                LOG.debug("Loaded offset: {}", apiOffset);
                apiOffset = lastRecordedOffset;
            }
        }
    }

    @Override
    public List<SourceRecord> poll() {

        if (System.currentTimeMillis() > (last_execution + pollMs)) {

            LOG.debug("Poll command: {}", command);

            last_execution = System.currentTimeMillis();
            String result = JRCommandExecutor.runTemplate(command);

            LOG.debug("Result: {}", result);

            List<SourceRecord>  sourceRecords = new ArrayList<>();
            Map<String, String> sourcePartition = Collections.singletonMap("filename", command);
            Map<String, Long> sourceOffset = Collections.singletonMap("position", ++apiOffset);
            sourceRecords.add(new SourceRecord(sourcePartition, sourceOffset, topic, Schema.STRING_SCHEMA, result));
            return sourceRecords;
        }
        return Collections.emptyList();
    }

    @Override
    public void stop() {}

}

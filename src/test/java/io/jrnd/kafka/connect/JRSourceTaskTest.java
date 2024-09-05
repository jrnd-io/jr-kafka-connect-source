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

package io.jrnd.kafka.connect;

import io.jrnd.kafka.connect.connector.JRCommandExecutor;
import io.jrnd.kafka.connect.connector.JRSourceConnector;
import io.jrnd.kafka.connect.connector.JRSourceTask;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class JRSourceTaskTest {

    @Mock
    private JRCommandExecutor executor;

    @Mock
    private SourceTaskContext context;

    @Mock
    private OffsetStorageReader offsetStorageReader;

    @InjectMocks
    private JRSourceTask jrSourceTask;

    private Map<String, String> config;

    @BeforeEach
    public void setUp() {

        config = new HashMap<>();
        config.put(JRSourceConnector.JR_EXISTING_TEMPLATE, "net_device");
        config.put(JRSourceConnector.TOPIC_CONFIG, "test-topic");
        config.put(JRSourceConnector.POLL_CONFIG, "1000");
        config.put(JRSourceConnector.OBJECTS_CONFIG, "10");
        config.put(JRSourceConnector.KEY_VALUE_LENGTH, "200");

        when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
    }

    @Test
    public void testStartWithOffset() {
        Map<String, Object> offset = new HashMap<>();
        offset.put("position", 5L);
        when(offsetStorageReader.offset(Collections.singletonMap("template", "net_device"))).thenReturn(offset);

        jrSourceTask.start(config);

        assertEquals("net_device", jrSourceTask.getTemplate());
        assertEquals("test-topic", jrSourceTask.getTopic());
        assertEquals(1000L, jrSourceTask.getPollMs());
        assertEquals(10, jrSourceTask.getObjects());
        assertEquals(5L, jrSourceTask.getApiOffset());
    }

    @Test
    public void testStartWithoutOffset() {
        when(offsetStorageReader.offset(Collections.singletonMap("template", "net_device"))).thenReturn(null);

        jrSourceTask.start(config);

        assertEquals("net_device", jrSourceTask.getTemplate());
        assertEquals("test-topic", jrSourceTask.getTopic());
        assertEquals(1000L, jrSourceTask.getPollMs());
        assertEquals(10, jrSourceTask.getObjects());
        assertEquals(0L, jrSourceTask.getApiOffset());
    }

    //@Test
    public void testPoll() {

        jrSourceTask.start(config);

        when(executor.runTemplate("net_device", 10, null, 100)).thenReturn(Arrays.asList("record1", "record2"));
        List<SourceRecord> records = jrSourceTask.poll();

        assertEquals(2, records.size());

        SourceRecord record1 = records.get(0);
        assertEquals("test-topic", record1.topic());
        assertEquals("record1", record1.value());

        SourceRecord record2 = records.get(1);
        assertEquals("test-topic", record2.topic());
        assertEquals("record2", record2.value());
    }

    @Test
    public void testPollNoExecution() {
        jrSourceTask.start(config);
        jrSourceTask.setLast_execution(System.currentTimeMillis());

        List<SourceRecord> records = jrSourceTask.poll();
        assertTrue(records.isEmpty());
    }

    @Test
    public void testCalculateApiOffset() {
        long currentLoopOffset = 5L;
        String newFromDate = "2024-01-01T00:00:00.0000000Z";
        String oldFromDate = "2024-01-01T00:00:00.0000000Z";

        long offset = jrSourceTask.calculateApiOffset(currentLoopOffset, newFromDate, oldFromDate);
        assertEquals(6L, offset);

        newFromDate = "2024-01-02T00:00:00.0000000Z";
        offset = jrSourceTask.calculateApiOffset(currentLoopOffset, newFromDate, oldFromDate);
        assertEquals(1L, offset);
    }
}

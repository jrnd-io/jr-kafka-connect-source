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

package io.jrnd.kafka.connect;

import io.jrnd.kafka.connect.connector.JRCommandExecutor;
import io.jrnd.kafka.connect.connector.JRSourceConnector;
import io.jrnd.kafka.connect.connector.JRSourceTask;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class JRSourceConnectorTest {

    @Mock
    private JRCommandExecutor jrCommandExecutor;

    @InjectMocks
    private JRSourceConnector jrSourceConnector;

    @BeforeEach
    public void setUp() {}

    @Test
    public void testStartValidConfig() {
        when(jrCommandExecutor.templates()).thenReturn(Arrays.asList("net_device", "gaming_game"));

        Map<String, String> config = new HashMap<>();
        config.put(JRSourceConnector.JR_EXISTING_TEMPLATE, "net_device");
        config.put(JRSourceConnector.TOPIC_CONFIG, "test-topic");
        config.put(JRSourceConnector.POLL_CONFIG, "1000");
        config.put(JRSourceConnector.OBJECTS_CONFIG, "10");

        jrSourceConnector.start(config);

        assertEquals("net_device", jrSourceConnector.getTemplate());
        assertEquals("test-topic", jrSourceConnector.getTopic());
        assertEquals(Long.valueOf(1000), jrSourceConnector.getPollMs());
        assertEquals(Integer.valueOf(10), jrSourceConnector.getObjects());
    }

    @Test
    public void testStartInvalidTemplate() {
        when(jrCommandExecutor.templates()).thenReturn(Arrays.asList("net_device", "gaming_game"));

        Map<String, String> config = new HashMap<>();
        config.put(JRSourceConnector.JR_EXISTING_TEMPLATE, "invalid_template");
        config.put(JRSourceConnector.TOPIC_CONFIG, "test-topic");
        config.put(JRSourceConnector.POLL_CONFIG, "1000");
        config.put(JRSourceConnector.OBJECTS_CONFIG, "10");

        ConfigException exception = assertThrows(ConfigException.class, () -> jrSourceConnector.start(config));
        assertEquals("'template' must be a valid JR template", exception.getMessage());
    }

    @Test
    public void testStartEmptyTemplates() throws Exception {
        when(jrCommandExecutor.templates()).thenReturn(Collections.emptyList());

        Map<String, String> config = new HashMap<>();
        config.put(JRSourceConnector.JR_EXISTING_TEMPLATE, "net_device");
        config.put(JRSourceConnector.TOPIC_CONFIG, "test-topic");
        config.put(JRSourceConnector.POLL_CONFIG, "1000");
        config.put(JRSourceConnector.OBJECTS_CONFIG, "10");

        ConfigException exception = assertThrows(ConfigException.class, () -> jrSourceConnector.start(config));
        assertEquals("JR template list is empty", exception.getMessage());
    }

    @Test
    public void testStartInvalidTopicConfig() {
        when(jrCommandExecutor.templates()).thenReturn(Arrays.asList("net_device", "gaming_game"));

        Map<String, String> config = new HashMap<>();
        config.put(JRSourceConnector.JR_EXISTING_TEMPLATE, "net_device");
        config.put(JRSourceConnector.TOPIC_CONFIG, "test-topic,another-topic");
        config.put(JRSourceConnector.POLL_CONFIG, "1000");
        config.put(JRSourceConnector.OBJECTS_CONFIG, "10");

        ConfigException exception = assertThrows(ConfigException.class, () -> jrSourceConnector.start(config));
        assertEquals("'topic' configuration requires definition of a single topic", exception.getMessage());
    }

    @Test
    public void testTaskClass() {
        Class<? extends Task> taskClass = jrSourceConnector.taskClass();
        assertEquals(JRSourceTask.class, taskClass);
    }

    @Test
    public void testTaskConfigs() {
        when(jrCommandExecutor.templates()).thenReturn(Arrays.asList("net_device", "gaming_game"));

        Map<String, String> config = new HashMap<>();
        config.put(JRSourceConnector.JR_EXISTING_TEMPLATE, "net_device");
        config.put(JRSourceConnector.TOPIC_CONFIG, "test-topic");
        config.put(JRSourceConnector.POLL_CONFIG, "1000");
        config.put(JRSourceConnector.OBJECTS_CONFIG, "10");

        jrSourceConnector.start(config);

        List<Map<String, String>> taskConfigs = jrSourceConnector.taskConfigs(1);
        assertNotNull(taskConfigs);
        assertEquals(1, taskConfigs.size());
        assertEquals("net_device", taskConfigs.get(0).get(JRSourceConnector.JR_EXISTING_TEMPLATE));
        assertEquals("test-topic", taskConfigs.get(0).get(JRSourceConnector.TOPIC_CONFIG));
        assertEquals("1000", taskConfigs.get(0).get(JRSourceConnector.POLL_CONFIG));
        assertEquals("10", taskConfigs.get(0).get(JRSourceConnector.OBJECTS_CONFIG));
    }
}

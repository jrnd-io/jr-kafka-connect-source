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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JRSourceConnector extends SourceConnector {

    private JRCommandExecutor jrCommandExecutor = JRCommandExecutor.getInstance();

    public static final String JR_EXISTING_TEMPLATE = "template";
    public static final String TOPIC_CONFIG = "topic";
    public static final String POLL_CONFIG = "frequency";
    public static final String OBJECTS_CONFIG = "objects";

    private static final String DEFAULT_TEMPLATE = "net_device";

    private String topic;
    private String template;
    private Long pollMs;
    private Integer objects;

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(JR_EXISTING_TEMPLATE, ConfigDef.Type.STRING, "net_device", ConfigDef.Importance.HIGH, "A valid JR existing template name.")
            .define(TOPIC_CONFIG, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "Topics to publish data to.")
            .define(POLL_CONFIG, ConfigDef.Type.LONG, ConfigDef.Importance.HIGH, "Repeat the creation every X milliseconds.")
            .define(OBJECTS_CONFIG, ConfigDef.Type.INT, 1, ConfigDef.Importance.HIGH, "Number of objects to create at every run.");

    private static final Logger LOG = LoggerFactory.getLogger(JRSourceConnector.class);

    @Override
    public void start(Map<String, String> map) {

        //check list of available templates
        List<String> templates = jrCommandExecutor.templates();
        if(templates.isEmpty())
            throw new ConfigException("JR template list is empty");

        AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, map);
        template = parsedConfig.getString(JR_EXISTING_TEMPLATE);
        if(template == null || template.isEmpty())
            template = DEFAULT_TEMPLATE;

        System.out.println(templates);

        if(!templates.contains(template))
            throw new ConfigException("'template' must be a valid JR template");

        List<String> topics = parsedConfig.getList(TOPIC_CONFIG);
        if (topics == null || topics.size() != 1) {
            throw new ConfigException("'topic' configuration requires definition of a single topic");
        }
        topic = topics.get(0);

        pollMs = parsedConfig.getLong(POLL_CONFIG);

        objects = parsedConfig.getInt(OBJECTS_CONFIG);
        if(objects == null || objects < 1)
            objects = 1;

        if (LOG.isInfoEnabled())
            LOG.info("Config: template: {} - topic: {} - frequency: {} - objects: {}", template, topic, pollMs, objects);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return JRSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        Map<String, String> config = new HashMap<>();
        config.put(JR_EXISTING_TEMPLATE, template);
        config.put(TOPIC_CONFIG, topic);
        config.put(POLL_CONFIG, String.valueOf(pollMs));
        config.put(OBJECTS_CONFIG, String.valueOf(objects));
        configs.add(config);
        return configs;
    }

    @Override
    public void stop() {}

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        return null;
    }

    public Integer getObjects() {
        return objects;
    }

    public Long getPollMs() {
        return pollMs;
    }

    public String getTemplate() {
        return template;
    }

    public String getTopic() {
        return topic;
    }

    public JRCommandExecutor getJrCommandExecutor() {
        return jrCommandExecutor;
    }
}

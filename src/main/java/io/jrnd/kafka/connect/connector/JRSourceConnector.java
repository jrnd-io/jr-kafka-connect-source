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
import org.apache.kafka.connect.storage.StringConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JRSourceConnector extends SourceConnector {

    public static final String JR_EXISTING_TEMPLATE = "template";
    public static final String EMBEDDED_TEMPLATE = "embedded_template";
    public static final String JR_EXECUTABLE_PATH = "jr_executable_path";
    public static final String TOPIC_CONFIG = "topic";
    public static final String POLL_CONFIG = "frequency";
    public static final String DURATION_CONFIG = "duration";
    public static final String OBJECTS_CONFIG = "objects";
    public static final String KEY_FIELD = "key_field_name";
    public static final String KEY_VALUE_INTERVAL_MAX = "key_value_interval_max";
    public static final String KEY_EMBEDDED_TEMPLATE = "key_embedded_template";
    public static final String VALUE_CONVERTER = "value.converter";
    public static final String KEY_CONVERTER = "key.converter";

    private static final String DEFAULT_TEMPLATE = "net_device";

    private String topic;
    private String template;
    private String embeddedTemplate;
    private Long pollMs;
    private Long durationMs;
    private Integer objects;
    private String keyField;
    private Integer keyValueIntervalMax;
    private String keyEmbeddedTemplate;
    private String jrExecutablePath;
    private String valueConverter;
    private String keyConverter;

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(JR_EXISTING_TEMPLATE, ConfigDef.Type.STRING, DEFAULT_TEMPLATE, ConfigDef.Importance.HIGH, "A valid JR existing template name.")
            .define(EMBEDDED_TEMPLATE, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, "Location of a file containing a valid custom JR template. This property will take precedence over 'template'.")
            .define(TOPIC_CONFIG, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "Topics to publish data to.")
            .define(POLL_CONFIG, ConfigDef.Type.LONG, 5000, ConfigDef.Importance.HIGH, "Repeat the creation every 'frequency' milliseconds.")
            .define(DURATION_CONFIG, ConfigDef.Type.LONG, -1, ConfigDef.Importance.MEDIUM, "Set a time bound to the entire object creation. The duration is calculated starting from the first run and is expressed in milliseconds. At least one run will always been scheduled, regardless of the value for duration.ms.")
            .define(OBJECTS_CONFIG, ConfigDef.Type.INT, 1, ConfigDef.Importance.HIGH, "Number of objects to create at every run.")
            .define(KEY_FIELD, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, "Name for key field, for example ID")
            .define(KEY_VALUE_INTERVAL_MAX, ConfigDef.Type.INT, 100, ConfigDef.Importance.MEDIUM, "Maximum interval value for key value, for example 150 (0 to key_value_interval_max). Default is 100.")
            .define(KEY_EMBEDDED_TEMPLATE, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, "Location of a file containing a valid custom JR template for key. This property will take precedence over 'key_field_name'.")
            .define(JR_EXECUTABLE_PATH, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, "Location for JR executable on workers.")
            .define(VALUE_CONVERTER, ConfigDef.Type.STRING, StringConverter.class.getName(), ConfigDef.Importance.MEDIUM, "one between org.apache.kafka.connect.storage.StringConverter, io.confluent.connect.avro.AvroConverter, io.confluent.connect.json.JsonSchemaConverter or io.confluent.connect.protobuf.ProtobufConverter")
            .define(KEY_CONVERTER, ConfigDef.Type.STRING, StringConverter.class.getName(), ConfigDef.Importance.MEDIUM, "one between org.apache.kafka.connect.storage.StringConverter, io.confluent.connect.avro.AvroConverter, io.confluent.connect.json.JsonSchemaConverter or io.confluent.connect.protobuf.ProtobufConverter");

    private static final Logger LOG = LoggerFactory.getLogger(JRSourceConnector.class);

    @Override
    public void start(Map<String, String> map) {

        AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, map);

        jrExecutablePath = parsedConfig.getString(JR_EXECUTABLE_PATH);
        JRCommandExecutor jrCommandExecutor = JRCommandExecutor.getInstance(jrExecutablePath);

        pollMs = parsedConfig.getLong(POLL_CONFIG);

        embeddedTemplate = readTemplate(parsedConfig, EMBEDDED_TEMPLATE);
        keyEmbeddedTemplate = readTemplate(parsedConfig, KEY_EMBEDDED_TEMPLATE);

        if((embeddedTemplate == null || embeddedTemplate.isEmpty())) {
            template = parsedConfig.getString(JR_EXISTING_TEMPLATE);
            if(template == null || template.isEmpty())
                template = DEFAULT_TEMPLATE;

            // list of available templates from JR exec
            List<String> templates = jrCommandExecutor.templates();
            if(templates.isEmpty())
                throw new ConfigException("JR template list is empty.");
            if(!templates.contains(template)) {
                throw new ConfigException("'template' must be a valid JR template.");
            }
        }

        // Connector supports only one target topic
        List<String> topics = parsedConfig.getList(TOPIC_CONFIG);
        if (topics == null || topics.size() != 1) {
            throw new ConfigException("'topic' configuration requires definition of a single topic.");
        }
        topic = topics.get(0);

        durationMs = parsedConfig.getLong(DURATION_CONFIG);
        if(durationMs == null || durationMs < 1)
            durationMs = -1L;

        objects = parsedConfig.getInt(OBJECTS_CONFIG);
        if(objects == null || objects < 1)
            objects = 1;

        keyField = parsedConfig.getString(KEY_FIELD);

        keyValueIntervalMax = parsedConfig.getInt(KEY_VALUE_INTERVAL_MAX);
        if(keyValueIntervalMax == null || keyValueIntervalMax < 1)
            keyValueIntervalMax = 100;

        valueConverter = parsedConfig.getString(VALUE_CONVERTER);
        if(valueConverter == null || valueConverter.isEmpty())
            valueConverter = StringConverter.class.getName();

        keyConverter = parsedConfig.getString(KEY_CONVERTER);
        if(keyConverter == null || keyConverter.isEmpty())
            keyConverter = StringConverter.class.getName();

        if (LOG.isInfoEnabled())
            LOG.info("Config: template: {} - embedded_template: {} - topic: {} - frequency: {} - duration: {} - objects: {} - key_name: {} - key_value_interval_max: {} - key_embedded_template: {}- executable path: {}",
                    template, embeddedTemplate, topic, pollMs, durationMs, objects, keyField, keyValueIntervalMax, keyEmbeddedTemplate, jrExecutablePath);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return JRSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        Map<String, String> config = new HashMap<>();
        if(template != null && !template.isEmpty())
            config.put(JR_EXISTING_TEMPLATE, template);
        if(embeddedTemplate != null && !embeddedTemplate.isEmpty())
            config.put(EMBEDDED_TEMPLATE, embeddedTemplate);
        if(keyEmbeddedTemplate != null && !keyEmbeddedTemplate.isEmpty())
            config.put(KEY_EMBEDDED_TEMPLATE, keyEmbeddedTemplate);
        config.put(TOPIC_CONFIG, topic);
        config.put(POLL_CONFIG, String.valueOf(pollMs));
        if(durationMs != null)
            config.put(DURATION_CONFIG, String.valueOf(durationMs));
        config.put(OBJECTS_CONFIG, String.valueOf(objects));
        if(keyField != null && !keyField.isEmpty())
            config.put(KEY_FIELD, keyField);
        if(keyValueIntervalMax != null)
            config.put(KEY_VALUE_INTERVAL_MAX, String.valueOf(keyValueIntervalMax));
        if(jrExecutablePath != null && !jrExecutablePath.isEmpty())
            config.put(JR_EXECUTABLE_PATH, jrExecutablePath);
        config.put(VALUE_CONVERTER, valueConverter);
        config.put(KEY_CONVERTER, keyConverter);
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

    private String readFileToString(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        return Files.readString(path);
    }

    private String readTemplate(AbstractConfig parsedConfig, String templateFileLocation) {
        String result = parsedConfig.getString(templateFileLocation);
        if (result != null && !result.isEmpty()) {
            try {
                result = readFileToString(templateFileLocation);
                result = result.replaceAll("[\\n\\r]", "");
            } catch (IOException e) {
                throw new RuntimeException("can't read template from file.");
            }
        }
        return result;
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

    public String geyKeyField() {
        return keyField;
    }

    public Integer getKeyValueIntervalMax() {
        return keyValueIntervalMax;
    }

    public String getJrExecutablePath() {
        return jrExecutablePath;
    }
}
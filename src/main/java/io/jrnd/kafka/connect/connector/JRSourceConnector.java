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

    public static final String JR_COMMAND_CONFIG = "jr-command";
    public static final String TOPIC_CONFIG = "topic";
    public static final String POLL_CONFIG = "poll.ms";

    private String topic;
    private String command;
    private Long pollMs;

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(JR_COMMAND_CONFIG, ConfigDef.Type.LIST, null, ConfigDef.Importance.HIGH, "JR command to execute")
            .define(TOPIC_CONFIG, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "The topic to publish data to")
            .define(POLL_CONFIG, ConfigDef.Type.LONG, ConfigDef.Importance.HIGH, "Poll interval");

    private static final Logger LOG = LoggerFactory.getLogger(JRSourceConnector.class);

    @Override
    public void start(Map<String, String> map) {
        AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, map);
        List<String> commands = parsedConfig.getList(JR_COMMAND_CONFIG);
        if (commands == null || commands.size() != 1) {
            throw new ConfigException("'jr-command' configuration requires definition of a single command");
        }
        command = commands.get(0);

        List<String> topics = parsedConfig.getList(TOPIC_CONFIG);
        if (topics == null || topics.size() != 1) {
            throw new ConfigException("'topic' configuration requires definition of a single topic");
        }
        topic = topics.get(0);

        pollMs = parsedConfig.getLong(POLL_CONFIG);

        LOG.info("Config: jr-command: {} - topic: {} - pollMs: {}", command, topic, pollMs);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return JRSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        Map<String, String> config = new HashMap<>();
        config.put(JR_COMMAND_CONFIG, command);
        config.put(TOPIC_CONFIG, topic);
        config.put(POLL_CONFIG, String.valueOf(pollMs));
        configs.add(config);
        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        return null;
    }
}

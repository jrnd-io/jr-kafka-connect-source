# jr-kafka-connect-source

JR Source Connector for Apache Kafka Connect

## Quickstart

Requirements on host machine:

 - Docker engine.
 - Java JDK v 17.x or higher.
 - Apche Maven v 3.x

Run JR Source Connector Demo with command:

```
bootstrap.sh
```

This will instatiate a Kafka cluster using docker containers with:

 - 1 broker listening on 9092
 - 1 schema registry listening on 8081
 - 1 kafka connect listening on 8083
 - JR binary already installed on kafka connect container
 - JR source connector plugin installed on kafka connect container

A JR connector job for template _net_device_ will be instantiated and produce a new random message to _net_device_ topic every 5 seconds.

```
{
    "name" : "jr-quickstart",
    "config": {
        "connector.class" : "io.jrnd.kafka.connect.connector.JRSourceConnector",
        "jr-command" : "jr run net_device",
        "topic": "net_device",
        "poll.ms" : 5000,
        "tasks.max": 1
    }
}
```

To shutdown the demo, run command:

```
tear-down.sh
```

## Configuration

JR Source Connector can be confgured with:

 - jr-command: A JR command to run. Accepted commands are: _jr run <template_name>_
 - topic: target topic
 - poll.ms: interval in milliseconds to generate a new message.

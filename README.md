# jr-kafka-connect-source

JR Source Connector for Apache Kafka Connect

## Quickstart

To run the demo, you will need on your host machine:

 - Docker engine.
 - Java JDK v 17.x or higher.
 - Apache Maven v 3.x

Run JR Source Connector demo with command:

```
bootstrap.sh
```

This will instatiate a Kafka cluster using docker containers with:

 - 1 broker listening on port 9092
 - 1 schema registry listening on port 8081
 - 1 kafka connect listening on port 8083
 - JR binary already installed on kafka connect container
 - JR source connector plugin installed on kafka connect container

A JR connector job for template _net_device_ will be instantiated and produce a new random message to _net_device_ topic every 5 seconds.

```
{
    "name" : "jr-quickstart",
    "config": {
        "connector.class" : "io.jrnd.kafka.connect.connector.JRSourceConnector",
        "template" : "net_device",
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

 - _template_: A valid JR existing template name. For a list of template see: https://jrnd.io/docs/#listing-existing-templates
 - _topic_: target topic
 - _poll.ms_: interval in milliseconds to generate a new message.

# jr-kafka-connect-source

JR Source Connector for Apache Kafka Connect.

JR executable should be installed on Kafka Connect Worker nodes to run the connector. (see Quickstart for an example).

Instructions on how to install JR on a target host are available at: https://jrnd.io

## Quickstart

> [!NOTE]  
> At the moment Quickstart runs only on x86-64 architectures.

In order to run JR Source Connector Quickstart, you will need on your host machine:

 - Docker engine.
 - Java JDK v 17.x or higher.
 - Apache Maven v 3.x

Quickstart is placed in _quickstart_ folder.

Run JR Source Connector Quickstart with command:

```
bootstrap.sh
```

This will instantiate a Kafka cluster using docker containers with:

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
        "frequency" : 5000,
        "tasks.max": 1
    }
}
```

To shut down JR Source Connector Quickstart, run command:

```
tear-down.sh
```

## Configuration

JR Source Connector can be configured with:

 - _template_: A valid JR existing template name. For a list of template see: https://jrnd.io/docs/#listing-existing-templates
 - _topic_: target topic
 - _frequency_: repeat the creation every X milliseconds.

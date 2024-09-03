# jr-kafka-connect-source

JR Source Connector for Apache Kafka Connect.

> [!NOTE]  
> JR executable should be installed on Kafka Connect Worker nodes to run the connector _(see Quickstart for an example)_. Instructions on how to install JR on a target host are available at: https://jrnd.io

## Quickstart

In order to run JR Source Connector Quickstart, you will need on your host machine:

 - Docker engine.
 - Java JDK v 17.x or higher.
 - Apache Maven v 3.x

Quickstart is placed in _quickstart_ folder.

Run JR Source Connector Quickstart from inside  _quickstart_ folder with command:

```
bootstrap.sh
```

This will instantiate a Kafka cluster using docker containers with:

 - 1 broker listening on port 9092
 - 1 schema registry listening on port 8081
 - 1 kafka connect listening on port 8083
 - JR binary already installed on kafka connect container
 - JR source connector plugin installed on kafka connect container

A JR connector job for template _net_device_ will be instantiated and produce 5 new random messages to _net_device_ topic every 5 seconds.

```
{
    "name" : "jr-quickstart",
    "config": {
        "connector.class" : "io.jrnd.kafka.connect.connector.JRSourceConnector",
        "template" : "net_device",
        "topic": "net_device",
        "frequency" : 5000,
        "objects": 5,
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

 - _template_: A valid JR existing template name. For a list of available templates see: https://jrnd.io/docs/#listing-existing-templates
 - _topic_: target topic
 - _frequency_: Repeat the creation of a random message every X milliseconds.
 - _objects_: Number of objects to create at every run. Default is 1.

## Install the connector

 - Download and extract the ZIP file from [releases](https://github.com/jrnd-io/jr-kafka-connect-source/releases)
 - Extract the ZIP file contents and copy the contents to the desired location on every Kafka Connect worker nodes, for example _/home/connect/jr_
 - Install JR executable on every Kafka Connect worker nodes, for example _brew install jr_
 - Add the folder to the plugin path in Kafka Connect properties file, for example, _plugin.path=/usr/local/share/kafka/plugins,/home/connect/jr_
 - Restart Kafka Connect worker nodes

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

```
kafka-console-consumer --bootstrap-server localhost:9092 --topic net_device --from-beginning --property print.key=true
null	{"VLAN": "BETA","IPV4_SRC_ADDR": "10.1.98.6","IPV4_DST_ADDR": "10.1.185.254","IN_BYTES": 1756,"FIRST_SWITCHED": 1724287965,"LAST_SWITCHED": 1725353374,"L4_SRC_PORT": 80,"L4_DST_PORT": 443,"TCP_FLAGS": 0,"PROTOCOL": 3,"SRC_TOS": 190,"SRC_AS": 1,"DST_AS": 1,"L7_PROTO": 81,"L7_PROTO_NAME": "TCP","L7_PROTO_CATEGORY": "Transport"}
null	{"VLAN": "BETA","IPV4_SRC_ADDR": "10.1.95.4","IPV4_DST_ADDR": "10.1.239.68","IN_BYTES": 1592,"FIRST_SWITCHED": 1722620372,"LAST_SWITCHED": 1724586369,"L4_SRC_PORT": 443,"L4_DST_PORT": 22,"TCP_FLAGS": 0,"PROTOCOL": 0,"SRC_TOS": 165,"SRC_AS": 3,"DST_AS": 1,"L7_PROTO": 443,"L7_PROTO_NAME": "HTTP","L7_PROTO_CATEGORY": "Transport"}
null	{"VLAN": "DELTA","IPV4_SRC_ADDR": "10.1.126.149","IPV4_DST_ADDR": "10.1.219.156","IN_BYTES": 1767,"FIRST_SWITCHED": 1721931269,"LAST_SWITCHED": 1724976862,"L4_SRC_PORT": 631,"L4_DST_PORT": 80,"TCP_FLAGS": 0,"PROTOCOL": 1,"SRC_TOS": 139,"SRC_AS": 0,"DST_AS": 1,"L7_PROTO": 22,"L7_PROTO_NAME": "TCP","L7_PROTO_CATEGORY": "Application"}
```

To shut down JR Source Connector Quickstart, run command:

```
tear-down.sh
```

## Configuration

JR Source Connector can be configured with:

 - _template_: A valid JR existing template name. For a list of available templates see: https://jrnd.io/docs/#listing-existing-templates
 - _topic_: target topic
 - _frequency_: Repeat the creation of a random object every X milliseconds.
 - _objects_: Number of objects to create at every run. Default is 1.
- _key_field_name_: Name for key field, for example 'ID'. This is an _OPTIONAL_ config, if not set, objects will be created without a key. Value for key will be calculated using JR function _key_, https://jrnd.io/docs/functions/#key
- _key_value_length_: Length for key value, for example 150. This is an _OPTIONAL_ config, if not set, length will be 100.
- _jr_executable_path_: Location for JR executable on workers. If not set, jr executable will be searched using $PATH variable.
  
## Examples

A JR connector job for template _users_ will be instantiated and produce 5 new random messages to _users_ topic every 5 seconds, using a message key field named USERID set with a random integer value between 0 and 150.

```
{
    "name" : "jr-keys-quickstart",
    "config": {
        "connector.class" : "io.jrnd.kafka.connect.connector.JRSourceConnector",
        "template" : "users",
        "topic": "users",
        "frequency" : 5000,
        "objects": 5,
        "key_field_name": "USERID",
        "key_value_length": 150,
        "jr_executable_path": "/usr/bin",
        "tasks.max": 1
    }
}
```

```
kafka-console-consumer --bootstrap-server localhost:9092 --topic users --from-beginning --property print.key=true
{"USERID":40}	{    "registertime": 1490191925954,    "USERID":40,    "regionid": "Region_1",    "gender": "MALE"}
{"USERID":53}	{    "registertime": 1490996658353,    "USERID":53,    "regionid": "Region_8",    "gender": "FEMALE"}
{"USERID":61}	{    "registertime": 1491758270753,    "USERID":61,    "regionid": "Region_8",    "gender": "FEMALE"}
{"USERID":86}	{    "registertime": 1515055706490,    "USERID":86,    "regionid": "Region_6",    "gender": "MALE"}
{"USERID":71}	{    "registertime": 1491441559667,    "USERID":71,    "regionid": "Region_6",    "gender": "OTHER"}
```


## Install the connector

 - Download and extract the ZIP file from [releases](https://github.com/jrnd-io/jr-kafka-connect-source/releases)
 - Extract the ZIP file contents and copy the contents to the desired location on every Kafka Connect worker nodes, for example _/home/connect/jr_
 - Install JR executable on every Kafka Connect worker nodes, for example _brew install jr_
 - Add the folder to the plugin path in Kafka Connect properties file, for example, _plugin.path=/usr/local/share/kafka/plugins,/home/connect/jr_
 - Restart Kafka Connect worker nodes

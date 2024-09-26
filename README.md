# jr-kafka-connect-source

JR Source Connector for Apache Kafka Connect.

JR (jrnd.io) is a CLI program that helps you to stream quality random data for your applications.

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
 - JR binary installed on kafka connect container
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
Consume from _net_device_ topic:

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

Parameter | Description                                                                                                                                                                                                                                                         | Default
-|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-
`template` | A valid JR existing template name. For a list of available templates see: https://jrnd.io/docs/#listing-existing-templates                                                                                                                                          | net_device
`embedded_template` | Location of a file containing a valid custom JR template. This property will take precedence over _template_. File must exist on Kafka Connect Worker nodes.                                                                                                        | 
`topic` | destination topic on Kafka                                                                                                                                                                                                                                          |
`frequency` | Repeat the creation of a random object every 'frequency' milliseconds.                                                                                                                                                                                              | 5000                                                                         
`duration` | Set a time bound to the entire object creation. The duration is calculated starting from the first run and is expressed in milliseconds. At least one run will always been scheduled, regardless of the value for 'duration'. If not set creation will run forever. | -1                                                                                              
`objects` | Number of objects to create at every run.                                                                                                                                                                                                                           | 1                                                                                                                                   
`key_field_name` | Name for key field, for example 'ID'. This is an _OPTIONAL_ config, if not set, objects will be created without a key. Value for key will be calculated using JR function _key_, https://jrnd.io/docs/functions/#key                                                |
`key_value_interval_max` | Maximum interval value for key value, for example 150 (0 to key_value_interval_max).                                                                                                                                                                                | 100
`jr_executable_path` | Location for JR executable on workers. If not set, jr executable will be searched using $PATH variable.                                                                                                                                                             |
`value.converter` | one between _org.apache.kafka.connect.storage.StringConverter_, _io.confluent.connect.avro.AvroConverter_, _io.confluent.connect.json.JsonSchemaConverter_ or _io.confluent.connect.protobuf.ProtobufConverter_                                                     |
`value.converter.schema.registry.url` | Only if _value.converter_ is set to _io.confluent.connect.avro.AvroConverter_, _io.confluent.connect.json.JsonSchemaConverter_ or _io.confluent.connect.protobuf.ProtobufConverter_. URL for _Confluent Schema Registry._                                           |

> [!NOTE]  
> At the moment for keys (_key.converter_) the supported format is _org.apache.kafka.connect.storage.StringConverter_.
For values there is also support for _Confluent Schema Registry_ with _Avro, Json and Protobuf schemas_.
  
## Examples

### Usage of keys

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
        "key_value_interval_max": 150,
        "jr_executable_path": "/usr/bin",
        "tasks.max": 1
    }
}
```
Consume from _users_ topic:

```
kafka-console-consumer --bootstrap-server localhost:9092 --topic users --from-beginning --property print.key=true

{"USERID":40}	{    "registertime": 1490191925954,    "USERID":40,    "regionid": "Region_1",    "gender": "MALE"}
{"USERID":53}	{    "registertime": 1490996658353,    "USERID":53,    "regionid": "Region_8",    "gender": "FEMALE"}
{"USERID":61}	{    "registertime": 1491758270753,    "USERID":61,    "regionid": "Region_8",    "gender": "FEMALE"}
{"USERID":86}	{    "registertime": 1515055706490,    "USERID":86,    "regionid": "Region_6",    "gender": "MALE"}
{"USERID":71}	{    "registertime": 1491441559667,    "USERID":71,    "regionid": "Region_6",    "gender": "OTHER"}
```

### Avro objects

A JR connector job for template _store_ will be instantiated and produce 5 new random messages to _store_ topic every 5 seconds, using the _Confluent Schema Registry_ to register the _Avro_ schema.

```
{
    "name" : "jr-avro-quickstart",
    "config": {
        "connector.class" : "io.jrnd.kafka.connect.connector.JRSourceConnector",
        "template" : "store",
        "topic": "store",
        "frequency" : 5000,
        "objects": 5,
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": "http://schema-registry:8081",
        "tasks.max": 1
    }
}
```

Consume from _store_ topic:

```
kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic store --from-beginning --property schema.registry.url=http://localhost:8081

{"store_id":1,"city":"Minneapolis","state":"AR"}
{"store_id":2,"city":"Baltimore","state":"LA"}
{"store_id":3,"city":"Chicago","state":"IL"}
{"store_id":4,"city":"Chicago","state":"MN"}
{"store_id":5,"city":"Washington","state":"OH"}
```

Show the _Avro_ schema registered:

```
curl -v http://localhost:8081/subjects/store-value/versions/1/schema
< HTTP/1.1 200 OK
< Content-Type: application/vnd.schemaregistry.v1+json


{"type":"record","name":"storeRecord","fields":[{"name":"store_id","type":"int"},{"name":"city","type":"string"},{"name":"state","type":"string"}],"connect.name":"storeRecord"}
```

### Json schema objects

A JR connector job for template _payment_credit_card_ will be instantiated and produce 5 new random messages to _payment_credit_card_ topic every 5 seconds, using the _Confluent Schema Registry_ to register the _Json_ schema.

```
{
    "name" : "jr-jsonschema-quickstart",
    "config": {
        "connector.class" : "io.jrnd.kafka.connect.connector.JRSourceConnector",
        "template" : "payment_credit_card",
        "topic": "payment_credit_card",
        "frequency" : 5000,
        "objects": 5,
        "value.converter": "io.confluent.connect.json.JsonSchemaConverter",
        "value.converter.schema.registry.url": "http://schema-registry:8081",
        "tasks.max": 1
    }
}
```

Consume from _payment_credit_card_ topic:

```
kafka-json-schema-console-consumer --bootstrap-server localhost:9092 --topic payment_credit_card --from-beginning --property schema.registry.url=http://localhost:8081

{"cvv":"070","card_number":"4086489674117803","expiration_date":"10/24","card_id":1.0}
{"cvv":"505","card_number":"346185299753204","expiration_date":"09/27","card_id":2.0}
{"cvv":"690","card_number":"47606709930001","expiration_date":"12/24","card_id":3.0}
{"cvv":"706","card_number":"4936815806226074","expiration_date":"08/24","card_id":4.0}
{"cvv":"855","card_number":"4782025916077384","expiration_date":"09/22","card_id":5.0}
```

Show the _Json_ schema registered:

```
curl -v http://localhost:8081/subjects/payment_credit_card-value/versions/1/schema
< HTTP/1.1 200 OK
< Content-Type: application/vnd.schemaregistry.v1+json


{"type":"object","properties":{"cvv":{"type":"string","connect.index":2},"card_number":{"type":"string","connect.index":1},"expiration_date":{"type":"string","connect.index":3},"card_id":{"type":"number","connect.index":0,"connect.type":"float64"}}}
```

### Protobuf objects

A JR connector job for template _shopping_rating_ will be instantiated and produce 5 new random messages to _shopping_rating_ topic every 5 seconds, using the _Confluent Schema Registry_ to register the _Protobuf_ schema.

```
{
    "name" : "jr-protobuf-quickstart",
    "config": {
        "connector.class" : "io.jrnd.kafka.connect.connector.JRSourceConnector",
        "template" : "shopping_rating",
        "topic": "shopping_rating",
        "frequency" : 5000,
        "objects": 5,
        "value.converter": "io.confluent.connect.protobuf.ProtobufConverter",
        "value.converter.schema.registry.url": "http://schema-registry:8081",
        "tasks.max": 1
    }
}
```

Consume from _shopping_rating_ topic:

```
kafka-protobuf-console-consumer --bootstrap-server localhost:9092 --topic shopping_rating --from-beginning --property schema.registry.url=http://localhost:8081

{"ratingId":1,"userId":0,"stars":2,"routeId":2348,"ratingTime":1,"channel":"iOS-test","message":"thank you for the most friendly,helpful experience today at your new lounge"}
{"ratingId":2,"userId":0,"stars":1,"routeId":6729,"ratingTime":13,"channel":"iOS","message":"why is it so difficult to keep the bathrooms clean ?"}
{"ratingId":3,"userId":0,"stars":3,"routeId":1137,"ratingTime":25,"channel":"ios","message":"Surprisingly good,maybe you are getting your mojo back at long last!"}
{"ratingId":4,"userId":0,"stars":2,"routeId":7306,"ratingTime":37,"channel":"android","message":"worst. flight. ever. #neveragain"}
{"ratingId":5,"userId":0,"stars":3,"routeId":2982,"ratingTime":49,"channel":"android","message":"meh"}
```

Show the _Protobuf_ schema registered:

```
curl -v http://localhost:8081/subjects/shopping_rating-value/versions/1/schema
< HTTP/1.1 200 OK
< Content-Type: application/vnd.schemaregistry.v1+json


syntax = "proto3";

message shopping_rating {
  int32 rating_id = 1;
  int32 user_id = 2;
  int32 stars = 3;
  int32 route_id = 4;
  int32 rating_time = 5;
  string channel = 6;
  string message = 7;
}
```

### Custom template

A JR connector job with a custom template will be instantiated and produce 5 new random messages to _customer_ topic every 5 seconds, using the _Confluent Schema Registry_ to register the _Avro_ schema.
Template definition is loaded from file _/tmp/customer-template.json_.

```
{
    "name" : "jr-avro-custom-quickstart",
    "config": {
        "connector.class" : "io.jrnd.kafka.connect.connector.JRSourceConnector",
        "embedded_template" : "/tmp/customer-template.json",
        "topic": "customer",
        "frequency" : 5000,
        "objects": 5,
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": "http://schema-registry:8081",
        "tasks.max": 1
    }
}
```

Consume from _customer_ topic:

```
kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic customer --from-beginning --property schema.registry.url=http://localhost:8081

{"customer_id":"6775933f-89c2-43b0-9eaf-e52e5f23293c","first_name":"Cynthia","last_name":"Foster","email":"cynthia.foster@hotmail.com","phone_number":"623 27678252","street_address":"Louisville, Cedar Lane 99, 21401","state":"Massachusetts","zip_code":"21401","country":"United States","country_code":"US"}
{"customer_id":"a15f891e-a3e7-4720-bf59-28202596c667","first_name":"Zachary","last_name":"Harris","email":"zachary.harris@aol.com","phone_number":"747 95821702","street_address":"Austin, River Road 8, 99801","state":"Illinois","zip_code":"99801","country":"United States","country_code":"US"}
{"customer_id":"8906111f-d6d3-4115-bd1a-3e231e3caaa2","first_name":"Julie","last_name":"Long","email":"julie.long@email.com","phone_number":"718 08720661","street_address":"Raleigh, Peachtree Street 43, 58501","state":"Georgia","zip_code":"58501","country":"United States","country_code":"US"}
{"customer_id":"9864ef53-eadf-4012-9cd0-c79e755169df","first_name":"Bryan","last_name":"Wilson","email":"bryan.wilson@mac.com","phone_number":"984 61669636","street_address":"San Antonio, Juniper Drive 23, 17101","state":"Illinois","zip_code":"17101","country":"United States","country_code":"US"}
{"customer_id":"a57911e5-dc9e-4da4-b280-1c0b0143538e","first_name":"Charles","last_name":"Thompson","email":"charles.thompson@gmail.com","phone_number":"726 39040449","street_address":"Richmond, Hillcrest Road 6, 43215","state":"Indiana","zip_code":"43215","country":"United States","country_code":"US"}
```

Show the _Avro_ schema registered:

```
curl -v http://localhost:8081/subjects/customer-value/versions/1/schema
< HTTP/1.1 200 OK
< Content-Type: application/vnd.schemaregistry.v1+json


{"type":"record","name":"recordRecord","fields":[{"name":"customer_id","type":"string"},{"name":"first_name","type":"string"},{"name":"last_name","type":"string"},{"name":"email","type":"string"},{"name":"phone_number","type":"string"},{"name":"street_address","type":"string"},{"name":"state","type":"string"},{"name":"zip_code","type":"string"},{"name":"country","type":"string"},{"name":"country_code","type":"string"}],"connect.name":"recordRecord"}
```

### Usage of duration

A JR connector job for template _marketing_campaign_finance_ will be instantiated and produce 5 new random messages to _users_ topic every 10 seconds for a total duration of 30 seconds.

```
{
    "name" : "jr-duration-quickstart",
    "config": {
        "connector.class" : "io.jrnd.kafka.connect.connector.JRSourceConnector",
        "template" : "marketing_campaign_finance",
        "topic": "marketing_campaign_finance",
        "frequency" : 10000,
        "duration" : 30000,
        "objects": 5,
        "tasks.max": 1
    }
}
```

Consume from _marketing_campaign_finance_ topic:

```
kafka-console-consumer --bootstrap-server localhost:9092 --topic marketing_campaign_finance --from-beginning

{  "time": 1610894253695,  "candidate_id": "A3272238",  "party_affiliation": "DEM",  "contribution": 1684}
{  "time": 1614389092497,  "candidate_id": "G8822487",  "party_affiliation": "DEM",  "contribution": 3166}
{  "time": 1600022334958,  "candidate_id": "G5165512",  "party_affiliation": "REP",  "contribution": 2933}
{  "time": 1594525458073,  "candidate_id": "X2839265",  "party_affiliation": "DEM",  "contribution": 824}
{  "time": 1606508742842,  "candidate_id": "T5688428",  "party_affiliation": "IND",  "contribution": 966}
{  "time": 1614055215125,  "candidate_id": "E4299542",  "party_affiliation": "DEM",  "contribution": 1240}
{  "time": 1610035678542,  "candidate_id": "H9769974",  "party_affiliation": "IND",  "contribution": 1793}
{  "time": 1609662702352,  "candidate_id": "S2314618",  "party_affiliation": "DEM",  "contribution": 1531}
{  "time": 1601632523200,  "candidate_id": "A8111647",  "party_affiliation": "IND",  "contribution": 2650}
{  "time": 1612493464065,  "candidate_id": "B1157343",  "party_affiliation": "DEM",  "contribution": 628}
{  "time": 1617678398100,  "candidate_id": "S7362235",  "party_affiliation": "REP",  "contribution": 3405}
{  "time": 1608939902703,  "candidate_id": "N9165865",  "party_affiliation": "REP",  "contribution": 1909}
{  "time": 1599100684111,  "candidate_id": "B2399959",  "party_affiliation": "REP",  "contribution": 1472}
{  "time": 1606312277382,  "candidate_id": "J1118736",  "party_affiliation": "IND",  "contribution": 1156}
{  "time": 1589668105856,  "candidate_id": "Q8211968",  "party_affiliation": "REP",  "contribution": 3457}

Processed a total of 15 messages
```

## Installation

### Manual

> [!NOTE]  
> JR executable should be installed on Kafka Connect Worker nodes to run the connector _(see Quickstart for an example)_. Instructions on how to install JR on a target host are available at: https://jrnd.io
> . A _docker compose_ with a predefined Kafka Connect cluster and JR is available in _quickstart_ folder. 

 - Download and extract the ZIP file from [releases](https://github.com/jrnd-io/jr-kafka-connect-source/releases).
 - Extract the ZIP file contents and copy the contents to the desired location on every Kafka Connect worker nodes, for example _/home/connect/jr_.
 - Install JR executable on every Kafka Connect worker nodes, for example _brew install jr_.
 - Add the folder to the plugin path in Kafka Connect properties file, for example, _plugin.path=/usr/local/share/kafka/plugins,/home/connect/jr_.
 - Restart Kafka Connect worker nodes.

### Confluent Hub

JR Source Connector is available on Confluent Hub: https://www.confluent.io/hub/jrndio/jr-source-connector

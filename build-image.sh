#!/bin/bash

echo "Create the connector archive..."
mvn clean package

echo "Create kafka-connect-demo-image docker image..."
docker build . -t jrndio/kafka-connect-demo-image:0.0.1
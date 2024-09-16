#!/bin/bash

DOCKERFILE=quickstart/Dockerfile
IMAGE_NAME=jrndio/kafka-connect-demo-image
IMAGE_VERSION=0.0.10

if [[ $(uname -m) == 'arm64' ]]; then
  DOCKERFILE=quickstart/Dockerfile-arm64
fi

cd ..

echo "Create the connector archive..."
mvn clean package

echo "Create kafka-connect-demo-image docker image..."

docker build -f $DOCKERFILE . -t $IMAGE_NAME:$IMAGE_VERSION

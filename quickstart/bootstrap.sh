#!/bin/bash

if [[ $(uname -m) == 'arm64' ]]; then
  echo "arm64 not yet supported for Quickstart"
  exit 1
fi

sh tear-down.sh

sh build-image.sh

sleep 3

echo "Starting docker containers..."
docker compose -f docker-compose.yml up -d

sleep 30

echo "Adding jr-source.quickstart job..."

curl -X POST -H Accept:application/json -H Content-Type:application/json \
  http://localhost:8083/connectors/ \
  -d @config/jr-source.quickstart.json

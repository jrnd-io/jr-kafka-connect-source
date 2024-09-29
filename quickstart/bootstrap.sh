#!/bin/bash

sh tear-down.sh

sh build-image.sh

sleep 3

echo "Starting docker containers..."
docker compose -f docker-compose.yml up -d

echo "Waiting 60 seconds for connect to be up..."

sleep 60

echo "Adding jr-source.quickstart job..."

curl -X POST -H Accept:application/json -H Content-Type:application/json \
  http://localhost:8083/connectors/ \
  -d @config/jr-source.quickstart.json

DIRECTORY="./config"
URL="http://localhost:8083/connectors/"

#for file in "$DIRECTORY"/*.json
#do
  #if [ -e "$file" ]; then
    #curl -X POST -H Accept:application/json -H Content-Type:application/json "$URL" \
         #-d @"$file"
  #fi
#done
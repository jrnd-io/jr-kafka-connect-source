#!/bin/bash

echo "Stopping docker containers..."
docker compose -f docker-compose.yml down --volumes

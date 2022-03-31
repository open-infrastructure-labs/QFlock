#!/bin/bash

source setup.sh

docker rmi -f $SPARK_DOCKER_BASE_NAME || true
docker rmi -f $SPARK_DOCKER_NAME || true
docker system prune -f
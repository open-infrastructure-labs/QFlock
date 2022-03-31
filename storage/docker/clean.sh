#!/bin/bash

DOCKER_NAME=qflock-storage
USER_NAME=${SUDO_USER:=$USER}

docker rmi -f $DOCKER_NAME || true
docker rmi -f $DOCKER_NAME-${USER_NAME} || true
docker system prune -f
rm docker/*.tar.gz

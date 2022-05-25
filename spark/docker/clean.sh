#!/bin/bash
set -e
pushd "$(dirname "$0")"

source setup.sh

if [ "${NO_CLEAN_DOCKERS}" != "1" ]; then
    echo "Spark is cleaning dockers"
    DOCKER_IMAGE_BASE=$(docker images -q $SPARK_DOCKER_BASE_NAME)
    if [ ! -z $DOCKER_IMAGE_BASE ]; then
        docker rmi -f $SPARK_DOCKER_BASE_NAME || true
    fi
    DOCKER_IMAGE=$(docker images -q $SPARK_DOCKER_NAME)
    if [ ! -z $DOCKER_IMAGE ]; then
        docker rmi -f $SPARK_DOCKER_NAME || true
    fi
    docker system prune -f
else
    echo "Spark is not cleaning dockers, NO_CLEAN_DOCKERS=1"
fi

if [ -e ./spark*.tgz ]; then
    rm -rf ./spark*.tgz
    rm -rf ./hadoop*.tar.gz
fi
popd
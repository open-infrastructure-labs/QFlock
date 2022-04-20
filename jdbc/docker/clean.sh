#!/bin/bash
pushd "$(dirname "$0")"

source setup.sh

if [ "${NO_CLEAN_DOCKERS}" != "1" ]; then
    echo "JDBC is cleaning dockers"
    DOCKER_IMAGE_BASE=$(docker images -q $JDBC_DOCKER_BASE_NAME)
    if [ ! -z $DOCKER_IMAGE_BASE ]; then
        docker rmi -f $JDBC_DOCKER_BASE_NAME || true
    fi
    DOCKER_IMAGE=$(docker images -q $JDBC_DOCKER_NAME)
    if [ ! -z $DOCKER_IMAGE ]; then
        docker rmi -f $JDBC_DOCKER_NAME || true
    fi
    docker system prune -f
else
    echo "JDBC is not cleaning dockers, NO_CLEAN_DOCKERS=1"
fi

if [ -e ./thrift*.tar.gz ]; then
    rm -rf ./thrift*.tar.gz
fi
popd

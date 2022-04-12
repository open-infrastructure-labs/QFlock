#!/bin/bash
pushd "$(dirname "$0")" # connect to root
ROOT_DIR=$(pwd)
echo "ROOT_DIR ${ROOT_DIR}"

source setup.sh

if [ "${NO_CLEAN_DOCKERS}" != "1" ]; then
    echo "JDBC is cleaning dockers"
    docker rmi -f $JDBC_DOCKER_BASE_NAME || true
    docker rmi -f $JDBC_DOCKER_NAME || true
    docker system prune -f
else
    echo "JDBC is not cleaning dockers, NO_CLEAN_DOCKERS=1"
fi

if [ -e ${ROOT_DIR}/thrift*.tar.gz ]; then
    rm -rf ${ROOT_DIR}/thrift*.tar.gz
fi
popd
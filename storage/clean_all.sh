#!/bin/bash
set -e
pushd "$(dirname "$0")" # connect to root
ROOT_DIR=$(pwd)
echo "ROOT_DIR ${ROOT_DIR}"
rm -rf ${ROOT_DIR}/volume
rm -rf ${ROOT_DIR}/data

DOCKER_NAME=qflock-storage
USER_NAME=${SUDO_USER:=$USER}

if [ "${NO_CLEAN_DOCKERS}" != "1" ]; then
    echo "storage is cleaning dockers"
    docker rmi -f $DOCKER_NAME || true
    docker rmi -f $DOCKER_NAME-${USER_NAME} || true
    docker system prune -f
else
    echo "storage is not cleaning dockers, NO_CLEAN_DOCKERS=1"
fi


rm ${ROOT_DIR}/docker/*.tar.gz || true
popd

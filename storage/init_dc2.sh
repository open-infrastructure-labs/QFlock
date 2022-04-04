#!/usr/bin/env bash

set -e # exit on error
pushd "$(dirname "$0")" # connect to root

ROOT_DIR=$(pwd)
echo "ROOT_DIR ${ROOT_DIR}"

USER_NAME=${SUDO_USER:=$USER}

# Make sure dc1 and dc2 are up and running

docker exec qflock-storage-dc1 bin/hadoop distcp hdfs://qflock-storage-dc1:9000/tpcds-parquet hdfs://qflock-storage-dc2:9000/



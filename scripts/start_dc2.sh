#!/usr/bin/env bash

set -e # exit on error
pushd "$(dirname "$0")" # connect to root
ROOT_DIR=$(pwd)
echo "ROOT_DIR ${ROOT_DIR}"

${ROOT_DIR}/init_networks.sh

pushd  ${ROOT_DIR}/../storage
./start_storage_dc.sh dc2
popd

# pushd ${ROOT_DIR}/../jdbc
# ./start.sh
# popd

pushd ${ROOT_DIR}/../spark/extensions/server
./start.sh
popd


#!/bin/bash

pushd "$(dirname "$0")" # connect to root
WORKING_DIR=$(pwd)
echo "WORKING_DIR ${WORKING_DIR}"

./clean.sh

rm -rf volume
${WORKING_DIR}/docker/clean.sh
popd
echo "jdbc clean all done"

#!/bin/bash
set -e
pushd "$(dirname "$0")" # connect to root
WORKING_DIR=$(pwd)
echo "WORKING_DIR ${WORKING_DIR}"

rm -rf ${WORKING_DIR}/build
pushd ${WORKING_DIR}/extensions
./clean.sh
popd
echo "spark clean done"

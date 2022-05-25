#!/bin/bash
set -e
pushd "$(dirname "$0")" # connect to root
WORKING_DIR=$(pwd)
echo "WORKING_DIR ${WORKING_DIR}"

./clean.sh
rm -rf ${WORKING_DIR}/volume
${WORKING_DIR}/docker/clean.sh
popd
echo "spark clean all done"

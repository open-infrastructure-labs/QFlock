#!/bin/bash

pushd "$(dirname "$0")" # connect to root
ROOT_DIR=$(pwd)
echo "ROOT_DIR ${ROOT_DIR}"

./clean.sh

rm -rf volume
${ROOT_DIR}/docker/clean.sh
popd
echo "jdbc clean all done"

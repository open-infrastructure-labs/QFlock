#!/bin/bash
set -e

pushd "$(dirname "$0")" # connect to root
ROOT_DIR=$(pwd)
echo "ROOT_DIR ${ROOT_DIR}"

pushd docker
./build.sh || (echo "*** jdbc build failed with $?" ; exit 1)
popd

pushd thrift
./build.sh
popd

pushd driver
./build.sh
popd

popd

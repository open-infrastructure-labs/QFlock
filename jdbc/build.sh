#!/bin/bash
set -e

pushd "$(dirname "$0")"

if [ ! -d build ]; then
  echo "Creating build directories"
  source docker/setup_build.sh
fi

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

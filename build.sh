#!/bin/bash
set -e
# Check if qflock network exists
if ! docker network ls | grep qflock-net; then
  docker network create qflock-net
fi

pushd storage/docker
./build.sh
popd

spark/docker/build.sh

pushd jdbc
./build.sh
popd

pushd spark
./build.sh
popd

pushd benchmark
./build.sh
popd


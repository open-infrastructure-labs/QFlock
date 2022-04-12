#!/bin/bash

# Check if qflock network exists
if ! docker network ls | grep qflock-net; then
  docker network create qflock-net
fi

pushd storage/docker
./build.sh
popd

pushd spark
./build.sh
popd

pushd benchmark
./build.sh
popd

pushd jdbc
./build.sh
popd


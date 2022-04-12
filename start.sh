#!/bin/bash

# Set up docker networks to simulate dc to dc interconnect
# Check if qflock network exists
if ! docker network ls | grep qflock-net; then
  docker network create qflock-net
fi

# Check if qflock network exists
if ! docker network ls | grep qflock-net-dc1; then
  docker network create qflock-net-dc1
fi

# Check if qflock network exists
if ! docker network ls | grep qflock-net-dc2; then
  docker network create qflock-net-dc2
fi


pushd storage
./start_storage_dc.sh dc1
./start_storage_dc.sh dc2
popd

pushd spark
./start.sh
popd

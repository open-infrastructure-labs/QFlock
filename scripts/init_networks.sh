#!/usr/bin/env bash

set -e # exit on error
pushd "$(dirname "$0")" # connect to root
ROOT_DIR=$(pwd)
echo "ROOT_DIR ${ROOT_DIR}"

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

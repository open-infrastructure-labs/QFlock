#!/bin/bash
set -e

pushd "$(dirname "$0")" # connect to root
ROOT_DIR=$(pwd)
echo "ROOT_DIR ${ROOT_DIR}"

pushd docker
./build.sh || (echo "*** Spark build failed with $?" ; exit 1)
popd

scripts/extract_spark.sh

mkdir -p spark

pushd extensions
./build.sh
popd

popd

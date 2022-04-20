#!/bin/bash
set -e

pushd "$(dirname "$0")"

if [ ! -d build ]; then
  echo "Creating build directories"
  source docker/setup_build.sh
fi

pushd docker
./build.sh || (echo "*** Spark build failed with $?" ; exit 1)
popd

scripts/extract_spark.sh

mkdir -p spark

pushd extensions
./build.sh
popd

popd

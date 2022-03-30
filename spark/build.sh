#!/bin/bash
pushd docker

./build.sh || (echo "*** Spark build failed with $?" ; exit 1)

popd
scripts/extract_spark.sh

mkdir -p spark

pushd extensions
./build.sh
popd

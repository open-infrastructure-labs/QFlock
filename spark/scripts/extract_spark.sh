#!/bin/bash
set -e
ROOT_DIR=$(git rev-parse --show-toplevel)
source $ROOT_DIR/scripts/spark/spark_version
source docker/setup.sh
echo $SPARK_VERSION
echo ${SPARK_PACKAGE}
tar -xzf docker/${SPARK_PACKAGE} -C build
rm -rf build/spark-${SPARK_VERSION}
mv build/${SPARK_PACKAGE_FOLDER} build/spark-${SPARK_VERSION}

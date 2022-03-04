#!/bin/bash

source docker/spark_version
source docker/setup.sh
echo $SPARK_VERSION

if [ ! -f ${SPARK_SRC_PACKAGE} ]
then
  echo "build.sh: Downloading ${SPARK_SRC_PACKAGE}"
  curl -L ${SPARK_SRC_PACKAGE_URL} --output ${SPARK_SRC_PACKAGE}
fi
if [ ! -f ${SPARK_SRC_PACKAGE} ]
then
  exit
fi
tar -xzf ${SPARK_SRC_PACKAGE}
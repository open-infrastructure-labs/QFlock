#!/bin/bash
set -e
pushd "$(dirname "$0")" # connect to root
ROOT_DIR=$(pwd)
echo "ROOT_DIR ${ROOT_DIR}"

pushd tpch-dbgen
make
popd

pushd tpcds-kit/tools
make
popd

BUILD_DIR=build
mkdir -p ${BUILD_DIR} || true
HIVE_PACKAGE="apache-hive-3.1.2-bin"
HIVE_PACKAGE_FILE="apache-hive-3.1.2-bin.tar.gz"
HIVE_PACKAGE_URL="https://dlcdn.apache.org/hive/hive-3.1.2/${HIVE_PACKAGE_FILE}"
JARS_DIR="src/jars"
if [ ! -f ${JARS_DIR} ]
then
  if [ ! -f ${BUILD_DIR}/${HIVE_PACKAGE_FILE} ]
  then
    echo "build.sh: Downloading ${HIVE_PACKAGE_FILE}"
    curl -L ${HIVE_PACKAGE_URL} --output ${BUILD_DIR}/${HIVE_PACKAGE_FILE}
  fi
  tar -xzf ${BUILD_DIR}/${HIVE_PACKAGE_FILE} -C ${BUILD_DIR}
  mkdir -p ${JARS_DIR} || true
  mv ${BUILD_DIR}/${HIVE_PACKAGE}/lib/*.jar ${JARS_DIR}
fi
popd
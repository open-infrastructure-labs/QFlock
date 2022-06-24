#!/bin/bash

pushd "$(dirname "$0")" # connect to root
WORKING_DIR=$(pwd)
echo "WORKING_DIR ${WORKING_DIR}"

rm -rf ${WORKING_DIR}/build

rm -rf ${WORKING_DIR}/jdbc/driver/build/
rm -rf ${WORKING_DIR}/jdbc/driver/project/
rm -rf ${WORKING_DIR}/jdbc/driver/target/
rm -rf ${WORKING_DIR}/jdbc/driver/thrift-jdbc-server.iml

echo "jdbc driver clean done"

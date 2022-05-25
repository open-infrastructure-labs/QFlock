#!/bin/bash
set -e
pushd "$(dirname "$0")" # connect to root
WORKING_DIR=$(pwd)
echo "WORKING_DIR ${WORKING_DIR}"

rm -rf ${WORKING_DIR}/build
rm -rf server/com
rm -rf driver/src/main/java/com/github/qflock/jdbc/api/

driver/clean.sh

echo "jdbc clean done"

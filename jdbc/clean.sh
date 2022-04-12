#!/bin/bash
pushd "$(dirname "$0")" # connect to root
ROOT_DIR=$(pwd)
echo "ROOT_DIR ${ROOT_DIR}"

rm -rf ${ROOT_DIR}/build
rm -rf server/com
rm -rf driver/src/main/java/com/github/qflock/jdbc/api/

driver/build.sh -c
echo "jdbc clean done"

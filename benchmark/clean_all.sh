#!/bin/bash
set -e
pushd "$(dirname "$0")" # connect to root
ROOT_DIR=$(pwd)
echo "ROOT_DIR ${ROOT_DIR}"

rm -rf ${ROOT_DIR}/build
rm -rf ${ROOT_DIR}/data
rm -rf ${ROOT_DIR}/src/jars
rm -rf ${ROOT_DIR}/src/logs


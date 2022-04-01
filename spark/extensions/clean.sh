#!/bin/bash

pushd "$(dirname "$0")" # connect to root
ROOT_DIR=$(pwd)
echo "ROOT_DIR ${ROOT_DIR}"

rm -rf ${ROOT_DIR}/build
rm -rf ${ROOT_DIR}/lib
rm -rf ${ROOT_DIR}/target
rm -rf ${ROOT_DIR}/project/target
rm -rf ${ROOT_DIR}/project/project
rm -rf ${ROOT_DIR}/pushdown-datasource/.bsp
echo "Done cleaning extensions"

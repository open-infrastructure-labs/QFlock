#!/bin/bash

pushd "$(dirname "$0")" # connect to root
ROOT_DIR=$(pwd)
echo "ROOT_DIR ${ROOT_DIR}"
rm -rf ${ROOT_DIR}/volume
popd


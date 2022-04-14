#!/bin/bash

pushd "$(dirname "$0")" # connect to root

rm -rf ./build
rm -rf ./lib
rm -rf ./target
rm -rf ./project/target
rm -rf ./project/project
rm -rf ./pushdown-datasource/.bsp
echo "Done cleaning extensions"

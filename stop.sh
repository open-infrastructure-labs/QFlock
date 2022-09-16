#!/bin/bash
pushd "$(dirname "$0")" # connect to root

pushd storage
./stop.sh
popd

pushd spark
./stop.sh
popd

# pushd jdbc
# ./stop.sh
# popd

pushd spark/extensions/server
./stop.sh
popd

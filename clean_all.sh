#!/bin/bash
set -e

pushd storage
./clean_all.sh
popd

pushd spark
./clean_all.sh
popd

pushd benchmark
./clean_all.sh
popd

pushd jdbc
./clean_all.sh
popd


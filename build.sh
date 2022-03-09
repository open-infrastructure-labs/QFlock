#!/bin/bash

pushd storage/docker
./build.sh
popd

pushd spark
./build.sh
popd

pushd benchmark
./build.sh
popd


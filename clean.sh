#!/bin/bash

pushd storage
./clean.sh
popd

pushd spark
./clean.sh
popd

pushd benchmark
./clean.sh
popd


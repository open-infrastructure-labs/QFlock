#!/bin/bash

pushd storage
./clean_all.sh
popd

pushd spark
./clean_all.sh
popd

pushd benchmark
./clean_all.sh
popd

#!/bin/bash

pushd storage
./stop.sh
popd

pushd spark
./stop.sh
popd

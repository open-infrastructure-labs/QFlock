#!/bin/bash

pushd spark
./clean.sh
popd

pushd jdbc
./clean.sh
popd


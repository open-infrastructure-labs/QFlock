#!/bin/bash

pushd storage
./start_storage_dc.sh dc1
popd

pushd spark
./start.sh
popd

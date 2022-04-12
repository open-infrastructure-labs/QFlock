#!/bin/bash

pushd storage
./start_storage_dc.sh dc1
./start_storage_dc.sh dc2
popd

pushd jdbc
./start.sh
popd

sleep 10

pushd spark
./start.sh
popd

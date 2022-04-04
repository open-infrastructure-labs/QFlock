#!/bin/bash

pushd storage
./start_storage_dc.sh dc1
./start_storage_dc.sh dc2
popd

pushd spark
./start.sh
popd

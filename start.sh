#!/bin/bash

pushd storage
./start_qflock_storage.sh
popd

pushd spark
./start.sh
popd

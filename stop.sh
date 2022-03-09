#!/bin/bash

pushd storage
./stop_qflock_storage.sh
popd

pushd spark
./stop.sh
popd

#!/bin/bash

rm -rf data
rm -rf volume
pushd docker
./clean.sh
popd

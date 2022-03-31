#!/bin/bash

rm -rf build
pushd extensions
./clean.sh
popd
echo "spark clean done"

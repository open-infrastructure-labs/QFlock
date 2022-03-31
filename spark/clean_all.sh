#!/bin/bash

./clean.sh
rm -rf volume
rm -rf docker/spark-*.tgz
echo "spark clean all done"
pushd docker
./clean.sh
popd

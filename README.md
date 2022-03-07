# QFlock

# One time setup

git submodule  init

git submodule update --recursive --progress

# build
pushd storage/docker
./build.sh
popd

pushd spark
./build.sh
popd

pushd benchmark
./build.sh
popd

# start
pushd storage
./start_qflock_storage.sh
popd

pushd spark
./start.sh
popd

# benchmark/src/docker-bench.py is the script for configuring and running benchmarks.
# see benchmark/src/docker-bench.py --help for examples

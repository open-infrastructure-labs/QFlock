# QFlock

# One time setup

```shell
git submodule  init
git submodule update --recursive --progress
```

# build
```shell
pushd storage/docker
./build.sh
popd

pushd spark
./build.sh
popd

pushd benchmark
./build.sh
popd
```

# start
```shell
./start.sh
```
# stop
```shell
./stop.sh
```

# Configuring and running benchmarks.
```shell
# Examples
benchmark/src/docker-bench.py --help
```


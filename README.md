# QFlock

# One time setup

```shell
git submodule  init
git submodule update --recursive --progress
```

# build
```shell
./build.sh
```
# start
```shell
./start.sh
```
# stop
```shell
./stop.sh
```
# clean
```shell
./clean.sh
```
# reinitialize
Use these commands to stop services, remove all artifacts and rebuild the repo.
```shell
./stop.sh && ./clean.sh && ./build.sh
```

# Configuring and running benchmarks.
```shell
# Examples
- Initialize benchmark completely
benchmark/src/docker-bench.py --init

- Run all benchmark queries
benchmark/src/docker-bench.py --queries "*"

- Get help on available switches
benchmark/src/docker-bench.py --help
```


# QFlock

# One time setup

# Download build dependencies
```shell
sudo apt-get update
sudo apt-get install curl git docker.io build-essential bison flex
```

# Clone QFlock repository
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
# clean<BR>
to clean out build artifacts
```shell
./clean.sh
```
# clean all<BR>
cleans out build artifacts and all other artifacts including dockers and downloads
```shell
./clean_all.sh
```
# clean all but skip docker removal
```shell
NO_CLEAN_DOCKERS=1 ./clean_all.sh
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
./init.sh

- Run all benchmark queries
benchmark/src/docker-bench.py --query_range "*"

- Get help on available switches
benchmark/src/docker-bench.py --help
```

# Testing with Qflock Remote Server 
```shell
- Set metastore locations to be all on dc2 (helps with testing remote
python3 storage/metastore/qflock_metastore_one_location.py dc2
- Or split locations between dc2 and dc2 evenly
python3 storage/metastore/qflock_metastore_split_locations.py
- Run all benchmark queries against Qflock Remote Server
benchmark/src/docker-bench.py --query_range "*" -ext remote
```

#! /bin/bash
pushd "$(dirname "$0")" > /dev/null 2>&1 # connect to root
../../../stop.sh > ../logs/stop.txt 2>&1

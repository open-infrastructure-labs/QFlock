#! /bin/bash
pushd "$(dirname "$0")" > /dev/null 2>&1 # connect to root
../../../start.sh > ../logs/start.txt 2>&1

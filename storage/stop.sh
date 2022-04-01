#!/usr/bin/env bash

# docker stop qflock-storage
docker container stop $(docker container ls -q --filter name="qflock-storage*")


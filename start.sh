#!/bin/bash

./scripts/start_dc1.sh

./scripts/start_dc2.sh

sleep 1
# Add qflock network between datacenters
docker network connect qflock-net qflock-spark-dc1
docker network connect qflock-net qflock-storage-dc1
docker network connect qflock-net qflock-storage-dc2


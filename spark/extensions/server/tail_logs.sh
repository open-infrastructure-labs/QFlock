#!/bin/bash

docker exec -it qflock-spark-dc2 tail -n 1000 -f /opt/volume/logs/server.log

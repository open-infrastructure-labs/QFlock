#!/bin/bash

docker exec -it qflock-jdbc-dc2 tail -n 1000 -f /opt/volume/logs/jdbc.log

#!/bin/bash

pushd /jdbc/server
./jdbc_server.py > /opt/volume/logs/jdbc.log 2>&1 &

echo "SPARK_MASTER_READY"
echo "SPARK_MASTER_READY" > /opt/volume/status/JDBC_STATE

echo "RUNNING_MODE $RUNNING_MODE"

if [ "$RUNNING_MODE" = "daemon" ]; then
    sleep infinity
fi
popd
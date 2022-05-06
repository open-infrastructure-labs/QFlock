#!/bin/bash

export HADOOP_CONF_DIR=/opt/spark-$SPARK_VERSION/conf/
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native

export CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath --glob)
#export HADOOP_ROOT_LOGGER=DEBUG,console
pushd /jdbc/server
./jdbc_server.py > /opt/volume/logs/jdbc.log 2>&1 &

echo "JDBC_READY"
echo "JDBC_READY" > /opt/volume/status/JDBC_STATE

echo "RUNNING_MODE $RUNNING_MODE"

if [ "$RUNNING_MODE" = "daemon" ]; then
    sleep infinity
fi
popd
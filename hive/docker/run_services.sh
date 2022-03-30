#!/bin/bash

set -e # exit on error

rm -f /opt/volume/status/HADOOP_STATE

if [ ! -f /opt/volume/namenode/current/VERSION ]; then
    "${HADOOP_HOME}/bin/hdfs" namenode -format
fi

export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native

echo "Starting Name Node ..."
"${HADOOP_HOME}/bin/hdfs" --daemon start namenode
echo "Starting Data Node ..."
"${HADOOP_HOME}/bin/hdfs" --daemon start datanode

export CLASSPATH=$(bin/hadoop classpath)
sleep 1

# Hive setup
export PATH=$PATH:$HIVE_HOME/bin
echo "Creating hive metastore directories..."
"${HADOOP_HOME}/bin/hdfs" dfs -mkdir -p /tmp
"${HADOOP_HOME}/bin/hdfs" dfs -chmod g+w /tmp
"${HADOOP_HOME}/bin/hdfs" dfs -mkdir -p /user/hive/warehouse
"${HADOOP_HOME}/bin/hdfs" dfs -chmod g+w /user/hive/warehouse

if [ ! -d /opt/volume/metastore/metastore_db ]; then
  "$HIVE_HOME/bin/schematool" -dbType derby -initSchema
fi

$HIVE_HOME/bin/hive --service metastore &
sleep 1

echo "HADOOP_READY"
echo "HADOOP_READY" > /opt/volume/status/HADOOP_STATE

echo "RUNNING_MODE $RUNNING_MODE"

if [ "$RUNNING_MODE" = "daemon" ]; then
    sleep infinity
fi

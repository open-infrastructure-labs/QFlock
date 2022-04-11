#!/bin/bash

set -e # exit on error

rm -f /opt/volume/status/HADOOP_STATE

export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native

export CLASSPATH=$(bin/hadoop classpath)


# Hive setup
export PATH=$PATH:$HIVE_HOME/bin
echo "Creating hive metastore directories..."
"${HADOOP_HOME}/bin/hdfs" dfs -mkdir -p /tmp
"${HADOOP_HOME}/bin/hdfs" dfs -chmod g+w /tmp
"${HADOOP_HOME}/bin/hdfs" dfs -mkdir -p /user/hive/warehouse
"${HADOOP_HOME}/bin/hdfs" dfs -chmod g+w /user/hive/warehouse

# $HIVE_HOME/bin/hive
# show databases;
# show tables from tpcds;

echo "HADOOP_READY"
echo "HADOOP_READY" > /opt/volume/status/HADOOP_STATE

echo "RUNNING_MODE $RUNNING_MODE"

if [ "$RUNNING_MODE" = "daemon" ]; then
    sleep infinity
fi

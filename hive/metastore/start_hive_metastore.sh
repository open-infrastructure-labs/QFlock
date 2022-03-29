#!/bin/bash

set -e # exit on error

rm -f /opt/volume/status/HADOOP_STATE

echo "creating hive metastore directories..."
"${HADOOP_HOME}/bin/hdfs" dfs -mkdir /tmp
"${HADOOP_HOME}/bin/hdfs" dfs -chmod g+w /tmp
"${HADOOP_HOME}/bin/hdfs" dfs -mkdir -p /user/hive/warehouse
"${HADOOP_HOME}/bin/hdfs" dfs -chmod g+w /user/hive/warehouse
"$HIVE_HOME/bin/schematool" -dbType derby -initSchema

export PATH=$PATH:$HIVE_HOME/bin

echo "starting hive metastore service..."
$HIVE_HOME/bin/hive --service metastore &


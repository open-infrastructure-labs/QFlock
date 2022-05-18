#!/bin/bash

set -e # exit on error

rm -f /opt/volume/status/HADOOP_STATE

export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native

export CLASSPATH=$(bin/hadoop classpath)

sudo service ssh restart

echo "Starting Data Node ..."
"${HADOOP_HOME}/bin/hdfs" --daemon start datanode
echo "start yarn Resource Manager"
${HADOOP_HOME}/bin/yarn --daemon start resourcemanager
echo "start yarn Node Manager"
${HADOOP_HOME}/bin/yarn --daemon start nodemanager

# Hive setup
export PATH=$PATH:$HIVE_HOME/bin
echo "Creating hive metastore directories..."
"${HADOOP_HOME}/bin/hdfs" dfs -mkdir -p /tmp
"${HADOOP_HOME}/bin/hdfs" dfs -chmod g+w /tmp
"${HADOOP_HOME}/bin/hdfs" dfs -mkdir -p /user/hive/warehouse
"${HADOOP_HOME}/bin/hdfs" dfs -chmod g+w /user/hive/warehouse

# Tez setup, with hadoop in one tar ball 
echo "Setting up Apache tez ..."
#"${HADOOP_HOME}/bin/hdfs" dfs -rm -r -f /app/tez-0.10.2-SNAPSHOT
#"${HADOOP_HOME}/bin/hdfs" dfs -mkdir -p /app/tez-0.10.2-SNAPSHOT
#"${HADOOP_HOME}/bin/hdfs" dfs -chmod g+w /app/tez-0.10.2-SNAPSHOT
#"${HADOOP_HOME}/bin/hadoop" fs -put ${TEZ_HOME}/jar/tez-0.10.2-SNAPSHOT.tar.gz /app/tez-0.10.2-SNAPSHOT/tez-0.10.2-SNAPSHOT.tar.gz
"${HADOOP_HOME}/bin/hdfs" dfs -rm -r -f /app/tez-0.10.1
"${HADOOP_HOME}/bin/hdfs" dfs -mkdir -p /app/tez-0.10.1
"${HADOOP_HOME}/bin/hdfs" dfs -chmod g+w /app/tez-0.10.1
"${HADOOP_HOME}/bin/hadoop" fs -put ${TEZ_HOME}/jar/tez-0.10.1.tar.gz /app/tez-0.10.1/tez-0.10.1.tar.gz


# $HIVE_HOME/bin/hive
# show databases;
# show tables from tpcds;

echo "HADOOP_READY"
echo "HADOOP_READY" > /opt/volume/status/HADOOP_STATE
echo "RUNNING_MODE $RUNNING_MODE"

if [ "$RUNNING_MODE" = "daemon" ]; then
    sleep infinity
fi

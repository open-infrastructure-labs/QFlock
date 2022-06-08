#!/bin/bash

set -e # exit on error

rm -f /opt/volume/status/HADOOP_STATE

export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native

export CLASSPATH=$(bin/hadoop classpath)

echo "Start ssh service"
sudo service ssh restart

echo "Starting Hive cluster Name Node ..."
if [ ! -f /opt/volume/namenode/current/VERSION ]; then
    $HADOOP_PREFIX/bin/hdfs namenode -format
fi
$HADOOP_PREFIX/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs start namenode
echo "Starting Data Node ..."
$HADOOP_PREFIX/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs start datanode
echo "start yarn Resource Manager"
$HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config $HADOOP_CONF_DIR start resourcemanager
echo "start yarn Node Manager"
ssh-keyscan -H localhost >> ~/.ssh/known_hosts
$HADOOP_YARN_HOME/sbin/yarn-daemons.sh --config $HADOOP_CONF_DIR start nodemanager

# Hive setup
export PATH=$PATH:$HIVE_HOME/bin
echo "Creating hive metastore directories if it has not done already..."
if [ ! -f /opt/volume/metastore/metastore_db/dbex.lck ]; then
  "${HADOOP_HOME}/bin/hdfs" dfs -mkdir /tmp
  "${HADOOP_HOME}/bin/hdfs" dfs -chmod g+w /tmp
  "${HADOOP_HOME}/bin/hdfs" dfs -mkdir -p /user/hive/warehouse
  "${HADOOP_HOME}/bin/hdfs" dfs -chmod g+w /user/hive/warehouse
  $HIVE_HOME/bin/schematool -dbType derby -initSchema
fi
$HIVE_HOME/bin/hive --service metastore &> /opt/volume/metastore/metastore.log &
sleep 1

# Tez setup, with hadoop in one tar ball 
#echo "Setting up Apache tez in the cluster..."
"${HADOOP_HOME}/bin/hdfs" dfs -rm -r -f /app/tez-0.9.2
"${HADOOP_HOME}/bin/hdfs" dfs -mkdir -p /app/tez-0.9.2
"${HADOOP_HOME}/bin/hdfs" dfs -chmod g+w /app/tez-0.9.2
"${HADOOP_HOME}/bin/hadoop" fs -put /tmp/tez/tez-dist/target/tez-0.9.2.tar.gz /app/tez-0.9.2/tez-0.9.2.tar.gz

# $HIVE_HOME/bin/hive
# show databases;
# show tables from tpcds;

echo "HADOOP_READY"
echo "HADOOP_READY" > /opt/volume/status/HADOOP_STATE
echo "RUNNING_MODE $RUNNING_MODE"

if [ "$RUNNING_MODE" = "daemon" ]; then
    sleep infinity
fi

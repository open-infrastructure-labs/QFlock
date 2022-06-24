#!/bin/bash

set -e # exit on error

rm -f /opt/volume/status/HADOOP_STATE

if [ ! -f /opt/volume/namenode/current/VERSION ]; then
    "${HADOOP_HOME}/bin/hdfs" namenode -format
    # $HIVE_HOME/bin/schematool -dbType derby -initSchema
fi

# Start ssh service
sudo service ssh start

export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native

echo "Starting Name Node ..."
"${HADOOP_HOME}/bin/hdfs" --daemon start namenode
echo "Starting Data Node ..."
"${HADOOP_HOME}/bin/hdfs" --daemon start datanode

export CLASSPATH=$(bin/hadoop classpath)
sleep 1

# Hive setup
# export PATH=$PATH:$HIVE_HOME/bin
# $HIVE_HOME/bin/hive --service metastore &> /opt/volume/metastore/metastore.log &
# sleep 1

# python3 ${HADOOP_HOME}/bin/metastore/hive_metastore_proxy.py &
python3 ${HADOOP_HOME}/bin/metastore/qflock_metastore_server.py &

echo "HADOOP_READY"
echo "HADOOP_READY" > /opt/volume/status/HADOOP_STATE

echo "RUNNING_MODE $RUNNING_MODE"

if [ "$RUNNING_MODE" = "daemon" ]; then
    sleep infinity
fi
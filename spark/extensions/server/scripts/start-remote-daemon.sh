#!/bin/bash

export HADOOP_CONF_DIR=/opt/spark-$SPARK_VERSION/conf/
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native

export CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath --glob)
#export HADOOP_ROOT_LOGGER=DEBUG,console
if [ $# -eq 0 ]; then
  echo "Starting SSH Server"
  sudo service ssh start
  echo "Starting SSH Server.  Done."
fi
pushd /server
./server.sh > /opt/volume/logs/server.log 2>&1 &

echo "SERVER_READY"
echo "SERVER_READY" > /opt/volume/status/SERVER_STATE

echo "RUNNING_MODE $RUNNING_MODE"

if [ "$RUNNING_MODE" = "daemon" ]; then
    sleep infinity
fi
popd

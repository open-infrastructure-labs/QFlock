#!/bin/bash

# Script to start up the spark docker.

pushd "$(dirname "$0")"

# first start ssh
echo "Starting SSH Server"
sudo service ssh start
echo "Starting SSH Server.  Done."

if [ ! -d /qflock/spark/build/spark-logs ]; then
  mkdir /qflock/spark/build/spark-logs
fi
$SPARK_HOME/sbin/start-history-server.sh --properties-file /opt/spark-3.3.0/conf/history-server.properties > /opt/volume/logs/history-server.log

# next pause the docker so it does not exit.
sleep infinity
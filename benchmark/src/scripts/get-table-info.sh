#!/bin/bash
set -e
export HADOOP_CONF_DIR=/opt/spark-$SPARK_VERSION/conf/
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native

export CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath --glob)
#export HADOOP_ROOT_LOGGER=DEBUG,console
DEST=" "
if [ $# -gt 0 ]; then
  DEST=$1
fi
echo "results dir is: $DEST"
python3 scripts/table_info.py $DEST
python3 scripts/query_info.py queries/tpcds $DEST
echo "Done generating table and query info"

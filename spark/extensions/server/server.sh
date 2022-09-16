#!/bin/bash

DEBUG=NO
POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    -d|--debug)
    DEBUG=YES
    shift # past argument
    ;;
    *)    # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    shift # past argument
    ;;
esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

echo "DEBUG"  = "${DEBUG}"

export HADOOP_CONF_DIR=/opt/spark-$SPARK_VERSION/conf/
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native

export CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath --glob)

if [ ${DEBUG} == "YES" ]; then
    spark-submit --master local[4] --total-executor-cores 1 \
    --class com.github.qflock.server.QflockRemoteServer \
    --conf "spark.driver.maxResultSize=16g"  \
    --conf "spark.driver.memory=16g"  \
    --conf "spark.executor.memory=16g"  \
    --conf "spark.sql.execution.arrow.enabled=true"  \
    --conf "spark.sql.catalogImplementation=hive"  \
    --conf "spark.sql.warehouse.dir=hdfs://qflock-storage-dc1:9000/user/hive/warehouse3"  \
    --conf "spark.hadoop.dfs.blocksize=16777216"  \
    --conf "spark.hadoop.parquet.block.size=16777216"  \
    --conf "spark.sql.cbo.enabled=true"  \
    --conf "spark.sql.statistics.histogram.enabled=true"  \
    --jars /extensions/target/scala-2.12/qflock-extensions_2.12-0.1.0.jar \
    --conf spark.hadoop.hive.metastore.uris=thrift://qflock-storage-dc2:9084 \
    --conf 'spark.driver.extraJavaOptions=-classpath /conf/:/opt/spark-3.3.0/jars/*: -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=192.168.64.3:5006' \
    /extensions/target/scala-2.12/qflock-extensions_2.12-0.1.0.jar $@
else
    spark-submit --master local[4] --total-executor-cores 1 \
    --class com.github.qflock.server.QflockRemoteServer \
    --conf "spark.driver.maxResultSize=16g"  \
    --conf "spark.driver.memory=16g"  \
    --conf "spark.executor.memory=16g"  \
    --conf "spark.sql.execution.arrow.enabled=true"  \
    --conf "spark.sql.catalogImplementation=hive"  \
    --conf "spark.sql.warehouse.dir=hdfs://qflock-storage-dc1:9000/user/hive/warehouse3"  \
    --conf "spark.hadoop.dfs.blocksize=16777216"  \
    --conf "spark.hadoop.parquet.block.size=16777216"  \
    --conf "spark.sql.cbo.enabled=true"  \
    --conf "spark.sql.statistics.histogram.enabled=true"  \
    --conf 'spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/extensions/server/conf/log4j.properties' \
    --jars /extensions/target/scala-2.12/qflock-extensions_2.12-0.1.0.jar \
    --conf spark.hadoop.hive.metastore.uris=thrift://qflock-storage-dc2:9084 \
    /extensions/target/scala-2.12/qflock-extensions_2.12-0.1.0.jar $@
fi
#--conf 'spark.driver.extraJavaOptions=-classpath /conf/:/opt/spark-3.2.2/jars/*: -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=192.168.48.3:5006' \
#-Dlog4j.debug

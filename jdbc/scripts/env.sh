#!/bin/bash


export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop-2.7.4/

export HADOOP_CONF_DIR=/opt/spark-$SPARK_VERSION/conf/
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native

export CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath --glob)
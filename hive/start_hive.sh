#!/usr/bin/env bash

set -e # exit on error
pushd "$(dirname "$0")" # connect to root

ROOT_DIR=$(pwd)
echo "ROOT_DIR ${ROOT_DIR}"

if [ -z "$1" ]
  then
    DC=dc1
  else
    DC=$1
fi

USER_NAME=${SUDO_USER:=$USER}
# put this information into $GIT_ROOT/hive/hadoop_home/etc/hadoop/hadoop-env.sh
JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64

HADOOP_VERSION=2.7.2
HIVE_VERSION=2.3.8
TEZ_VERSION=0.9.2

HADOOP_HOME=/opt/hadoop/hadoop-${HADOOP_VERSION}
HIVE_HOME=/opt/hive/apache-hive-${HIVE_VERSION}-bin
TEZ_HOME=/opt/tez/apache-tez-${TEZ_VERSION}-bin
TEZ_CONF_DIR=${TEZ_HOME}/conf
TEZ_JARS=${TEZ_HOME}
HADOOP_CLASSPATH=${HIVE_HOME}/lib/*:${TEZ_CONF_DIR}:${TEZ_JARS}/*:${TEZ_JARS}/lib/*:${HADOOP_HOME}/etc/hadoop/:${HADOOP_HOME}/share/hadoop/common/*:${HADOOP_HOME}/share/hadoop/common/lib/*:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/lib/*:${HADOOP_HOME}/share/hadoop/yarn/*:${HADOOP_HOME}/share/hadoop/yarn/lib/*:
#HADOOP_CLASSPATH=${TEZ_CONF_DIR}:${TEZ_JARS}/*:${TEZ_JARS}/lib/*

# Create NameNode and DataNode mount points
# mkdir -p ${ROOT_DIR}/volume/namenode
mkdir -p ${ROOT_DIR}/volume/datanode0
mkdir -p ${ROOT_DIR}/volume/logs

# Create Hive metastore directory
mkdir -p ${ROOT_DIR}/volume/metastore

mkdir -p "${ROOT_DIR}/volume/status"
rm -f ${ROOT_DIR}/volume/status/*

# Can be used to transfer data to HDFS
mkdir -p ${ROOT_DIR}/data

CMD="bin/run_services.sh"
RUNNING_MODE="daemon"

if [ "$#" -ge 1 ] ; then
  CMD="$*"
  RUNNING_MODE="interactive"
fi

if [ "$RUNNING_MODE" = "interactive" ]; then
  DOCKER_IT="-i -t"
fi

# Check if hive network exists
# if ! docker network ls | grep qflock-net; then
#  docker network create qflock-net
#fi

HADOOP_ROOT_LOGGER=WARN,DRFA

DOCKER_RUN="docker run --rm=true ${DOCKER_IT} \
  -v ${ROOT_DIR}/data:/data \
  -v ${ROOT_DIR}/volume/namenode:/opt/volume/namenode \
  -v ${ROOT_DIR}/volume/datanode0:/opt/volume/datanode \
  -v ${ROOT_DIR}/volume/metastore:/opt/volume/metastore \
  -v ${ROOT_DIR}/volume/status:/opt/volume/status \
  -v ${ROOT_DIR}/volume/logs:${HADOOP_HOME}/logs \
  -v ${ROOT_DIR}/hadoop_home/etc/hadoop/core-site.xml:${HADOOP_HOME}/etc/hadoop/core-site.xml \
  -v ${ROOT_DIR}/hadoop_home/etc/hadoop/hdfs-site.xml:${HADOOP_HOME}/etc/hadoop/hdfs-site.xml \
  -v ${ROOT_DIR}/hadoop_home/etc/hadoop/yarn-site.xml:${HADOOP_HOME}/etc/hadoop/yarn-site.xml \
  -v ${ROOT_DIR}/hadoop_home/etc/hadoop/mapred-site.xml:${HADOOP_HOME}/etc/hadoop/mapred-site.xml \
  -v ${ROOT_DIR}/hadoop_home/etc/hadoop/hadoop-env.sh:${HADOOP_HOME}/etc/hadoop/hadoop-env.sh \
  -v ${ROOT_DIR}/hive_home/conf/hive-site.xml:${HIVE_HOME}/conf/hive-site.xml \
  -v ${ROOT_DIR}/tez_home/conf/tez-site.xml:${TEZ_HOME}/conf/tez-site.xml \
  -v ${ROOT_DIR}/docker/run_services.sh:${HADOOP_HOME}/bin/run_services.sh \
  -v ${ROOT_DIR}/docker/apache-tez-0.9.2-bin.tar.gz:${TEZ_HOME}/jar/apache-tez-0.9.2-bin.tar.gz \
  -v ${ROOT_DIR}/jar/guava-19.0.jar:${HIVE_HOME}/lib/guava-19.0.jar \
  -w ${HADOOP_HOME} \
  -e JAVA_HOME=${JAVA_HOME} \
  -e HADOOP_HOME=${HADOOP_HOME} \
  -e HADOOP_YARN_HOME=${HADOOP_HOME} \
  -e HADOOP_PREFIX=${HADOOP_HOME} \
  -e HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop \
  -e HIVE_HOME=${HIVE_HOME} \
  -e TEZ_HOME=${TEZ_HOME} \
  -e TEZ_CONF_DIR=${TEZ_CONF_DIR} \
  -e TEZ_JARS=${TEZ_JARS} \
  -e PATH=${HADOOP_HOME}/bin:${TEZ_HOME}/bin:${HIVE_HOME}/bin:${PATH} \
  -e HADOOP_CLASSPATH=${HADOOP_CLASSPATH} \
  -e RUNNING_MODE=${RUNNING_MODE} \
  -e HADOOP_ROOT_LOGGER=${HADOOP_ROOT_LOGGER} \
  -e HIVE_AUX_JARS_PATH=${HIVE_HOME}/lib \
  --network qflock-net-${DC} \
  --name qflock-hive-${HIVE_VERSION} --hostname qflock-hive \
  qflock-hive-${HIVE_VERSION}-${USER_NAME} ${CMD}"

# echo ${DOCKER_RUN}

if [ "$RUNNING_MODE" = "interactive" ]; then
  eval "${DOCKER_RUN}"
else
  eval "${DOCKER_RUN}" &
  while [ ! -f "${ROOT_DIR}/volume/status/HADOOP_STATE" ]; do
    sleep 1  
  done

  cat "${ROOT_DIR}/volume/status/HADOOP_STATE"
  #docker exec dikehdfs /server/dikeHDFS &
fi

popd


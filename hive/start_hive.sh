#!/usr/bin/env bash

set -e # exit on error
pushd "$(dirname "$0")" # connect to root

ROOT_DIR=$(pwd)
echo "ROOT_DIR ${ROOT_DIR}"

USER_NAME=${SUDO_USER:=$USER}

# Set the home directory in the Docker container.
HADOOP_HOME=/opt/hadoop/hadoop-3.3.0
HIVE_HOME=/opt/hive/apache-hive-3.1.2-bin

# Create NameNode and DataNode mount points
mkdir -p ${ROOT_DIR}/volume/namenode
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
if ! docker network ls | grep qflock-net; then
  docker network create qflock-net
fi

DOCKER_RUN="docker run --rm=true ${DOCKER_IT} \
  -v ${ROOT_DIR}/data:/data \
  -v ${ROOT_DIR}/volume/namenode:/opt/volume/namenode \
  -v ${ROOT_DIR}/volume/datanode0:/opt/volume/datanode \
  -v ${ROOT_DIR}/volume/metastore:/opt/volume/metastore \
  -v ${ROOT_DIR}/volume/status:/opt/volume/status \
  -v ${ROOT_DIR}/volume/logs:${HADOOP_HOME}/logs \
  -v ${ROOT_DIR}/hadoop_home/etc/hadoop/core-site.xml:${HADOOP_HOME}/etc/hadoop/core-site.xml \
  -v ${ROOT_DIR}/hadoop_home/etc/hadoop/hdfs-site.xml:${HADOOP_HOME}/etc/hadoop/hdfs-site.xml \
  -v ${ROOT_DIR}/hive_home/conf/hive-site.xml:${HIVE_HOME}/conf/hive-site.xml \
  -v ${ROOT_DIR}/docker/run_services.sh:${HADOOP_HOME}/bin/run_services.sh \
  -w ${HADOOP_HOME} \
  -e HADOOP_HOME=${HADOOP_HOME} \
  -e HIVE_HOME=${HIVE_HOME} \
  -e RUNNING_MODE=${RUNNING_MODE} \
  --network qflock-net \
  --name qflock-hive  --hostname qflock-hive \
  qflock-hive-${USER_NAME} ${CMD}"

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


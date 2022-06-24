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

if [ ! -f ${ROOT_DIR}/hadoop_home/etc/hadoop/core-site-${DC}.xml ]
  then
    echo "Error ${ROOT_DIR}/hadoop_home/etc/hadoop/core-site-${DC}.xml does not exist"
    exit 1
fi

USER_NAME=${SUDO_USER:=$USER}

# Set the home directory in the Docker container.
HADOOP_HOME=/opt/hadoop/hadoop-3.3.0
HIVE_HOME=/opt/hive/apache-hive-3.1.2-bin

# Create NameNode and DataNode mount points
mkdir -p ${ROOT_DIR}/volume/${DC}/namenode
mkdir -p ${ROOT_DIR}/volume/${DC}/datanode0
mkdir -p ${ROOT_DIR}/volume/${DC}/logs

# Create Hive metastore directory
mkdir -p ${ROOT_DIR}/volume/${DC}/metastore

# Create QFlock metastore directory
mkdir -p ${ROOT_DIR}/volume/${DC}/metastore/qflock

# Create QFlock default catalog directory
mkdir -p ${ROOT_DIR}/volume/${DC}/metastore/qflock/catalog/default

mkdir -p ${ROOT_DIR}/volume/${DC}/status
rm -f ${ROOT_DIR}/volume/${DC}/status/*

# Can be used to transfer data to HDFS
mkdir -p ${ROOT_DIR}/data

CMD="bin/run_services.sh"
RUNNING_MODE="daemon"

# We need to be absolutely sure that ssh configuration exists
mkdir -p ${ROOT_DIR}/volume/ssh
touch ${ROOT_DIR}/volume/ssh/authorized_keys
touch ${ROOT_DIR}/volume/ssh/config

DOCKER_RUN="docker run --rm=true ${DOCKER_IT} \
  -v ${ROOT_DIR}/data:/data \
  -v ${ROOT_DIR}/volume/ssh/authorized_keys:/home/${USER_NAME}/.ssh/authorized_keys \
  -v ${ROOT_DIR}/volume/ssh/config:/home/${USER_NAME}/.ssh/config \
  -v ${ROOT_DIR}/volume/${DC}/namenode:/opt/volume/namenode \
  -v ${ROOT_DIR}/volume/${DC}/datanode0:/opt/volume/datanode \
  -v ${ROOT_DIR}/volume/${DC}/metastore:/opt/volume/metastore \
  -v ${ROOT_DIR}/volume/${DC}/status:/opt/volume/status \
  -v ${ROOT_DIR}/volume/${DC}/logs:${HADOOP_HOME}/logs \
  -v ${ROOT_DIR}/hadoop_home/etc/hadoop/core-site-${DC}.xml:${HADOOP_HOME}/etc/hadoop/core-site.xml \
  -v ${ROOT_DIR}/hadoop_home/etc/hadoop/hdfs-site.xml:${HADOOP_HOME}/etc/hadoop/hdfs-site.xml \
  -v ${ROOT_DIR}/hive_home/conf/hive-site.xml:${HIVE_HOME}/conf/hive-site.xml \
  -v ${ROOT_DIR}/docker/run_services.sh:${HADOOP_HOME}/bin/run_services.sh \
  -v ${ROOT_DIR}/metastore:${HADOOP_HOME}/bin/metastore \
  -w ${HADOOP_HOME} \
  -e HADOOP_HOME=${HADOOP_HOME} \
  -e HIVE_HOME=${HIVE_HOME} \
  -e RUNNING_MODE=${RUNNING_MODE} \
  -e USER=${USER_NAME} \
  --network qflock-net-${DC} \
  --name qflock-storage-${DC}  --hostname qflock-storage-${DC} \
  qflock-storage-${USER_NAME} ${CMD}"

# echo ${DOCKER_RUN}

if [ "$RUNNING_MODE" = "interactive" ]; then
  eval "${DOCKER_RUN}"
else
  eval "${DOCKER_RUN}" &
  while [ ! -f "${ROOT_DIR}/volume/${DC}/status/HADOOP_STATE" ]; do
    sleep 1  
  done

  cat "${ROOT_DIR}/volume/${DC}/status/HADOOP_STATE"
fi

popd


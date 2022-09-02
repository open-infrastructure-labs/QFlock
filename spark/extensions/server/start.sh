#!/bin/bash

pushd "$(dirname "$0")"
ROOT_DIR=$(git rev-parse --show-toplevel)
SERVER_DIR=$ROOT_DIR/spark/extensions/server
DOCKER_DIR=$ROOT_DIR/spark/docker
SCRIPTS_DIR=$ROOT_DIR/scripts
source $DOCKER_DIR/setup.sh
source $ROOT_DIR/scripts/spark/spark_version

mkdir -p "${SERVER_DIR}/volume/logs"
rm -f "${SERVER_DIR}/volume/logs/*.log"

mkdir -p "${SERVER_DIR}/volume/status"
rm -f "${SERVER_DIR}/volume/status/server*"

# We need to be absolutely sure that ssh configuration exists
mkdir -p ${SERVER_DIR}/volume/ssh
touch ${SERVER_DIR}/volume/ssh/authorized_keys
touch ${SERVER_DIR}/volume/ssh/config

if [ ! -d $SERVER_DIR/build ]; then
  mkdir -p $SERVER_DIR/build/.ivy2
  mkdir -p $SERVER_DIR/build/.m2
  mkdir -p $SERVER_DIR/build/.cache
  mkdir -p $SERVER_DIR/build/.gnupg
  mkdir -p $SERVER_DIR/build/.sbt
fi

RUNNING_MODE="daemon"
START_LOCAL="YES"
# STORAGE_HOST1="--add-host=qflock-storage-dc1:$($SCRIPTS_DIR/get-docker-ip.py qflock-net-dc1 qflock-storage-dc1)"
STORAGE_HOST2="--add-host=qflock-storage-dc2:$($SCRIPTS_DIR/get-docker-ip.py qflock-net-dc2 qflock-storage-dc2)"
#DC2_SPARK_HOST="--add-host=qflock-dc2-spark:$($SCRIPTS_DIR/get-docker-ip.py qflock-dc2-spark)"
LOCAL_DOCKER_HOST="--add-host=local-docker-host:$($SCRIPTS_DIR/get-docker-ip.py qflock-net-dc2 qflock-net-dc2)"

echo "Local docker host ${LOCAL_DOCKER_HOST}"
echo "Storage ${STORAGE_HOST2}"

DOCKER_ID=""
if [ $RUNNING_MODE = "interactive" ]; then
  DOCKER_IT="-i -t"
fi
DOCKER_NAME="qflock-spark-dc2"
CMD="/server/scripts/start-remote-daemon.sh"
DOCKER_RUN="docker run ${DOCKER_IT} --rm \
  --name $DOCKER_NAME --hostname $DOCKER_NAME \
  $STORAGE_HOST2 $LOCAL_DOCKER_HOST\
  --network qflock-net-dc2 \
  -e MASTER=spark://sparkmaster:7077 \
  -e SPARK_CONF_DIR=/conf \
  -e SPARK_PUBLIC_DNS=localhost \
  -e SPARK_LOG_DIR=/opt/volume/logs \
  -e HADOOP_CONF_DIR=/opt/spark-$SPARK_VERSION/conf \
  --mount type=bind,source=$ROOT_DIR/,target=/qflock \
  --mount type=bind,source=$(pwd)/,target=/server \
  --mount type=bind,source=$(pwd)/scripts,target=/scripts \
  --mount type=bind,source=$ROOT_DIR/storage,target=/storage \
  --mount type=bind,source=$ROOT_DIR/spark/extensions,target=/extensions \
  -w /server \
  -v $ROOT_DIR/conf/spark:/opt/spark-$SPARK_VERSION/conf  \
  -v $ROOT_DIR/conf/hdfs-site.xml:/opt/spark-$SPARK_VERSION/conf/hdfs-site.xml  \
  -v $ROOT_DIR/conf/hive-site.xml:/opt/spark-$SPARK_VERSION/conf/hive-site.xml  \
  -v ${SERVER_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2 \
  -v ${SERVER_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg \
  -v ${SERVER_DIR}/build/.sbt:${DOCKER_HOME_DIR}/.sbt \
  -v ${SERVER_DIR}/build/.cache:${DOCKER_HOME_DIR}/.cache \
  -v ${SERVER_DIR}/build/.ivy2:${DOCKER_HOME_DIR}/.ivy2 \
  -v ${SERVER_DIR}/volume/status:/opt/volume/status \
  -v ${SERVER_DIR}/volume/logs:/opt/volume/logs \
  -v ${SERVER_DIR}/volume/ssh/authorized_keys:/home/${USER_NAME}/.ssh/authorized_keys \
  -v ${SERVER_DIR}/volume/ssh/config:/home/${USER_NAME}/.ssh/config \
  -e RUNNING_MODE=${RUNNING_MODE} \
  -u ${USER_ID} \
  ${SPARK_DOCKER_NAME} ${CMD}"

echo $DOCKER_RUN
echo "mode: $RUNNING_MODE"

if [ $RUNNING_MODE = "interactive" ]; then
  eval "${DOCKER_RUN}"
else
  eval "${DOCKER_RUN}" &
  while [ ! -f "${SERVER_DIR}/volume/status/SERVER_STATE" ]; do
    sleep 1
  done

  cat "${SERVER_DIR}/volume/status/SERVER_STATE"
fi

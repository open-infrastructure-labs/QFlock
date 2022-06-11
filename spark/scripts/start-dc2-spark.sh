#!/bin/bash

pushd "$(dirname "$0")"
ROOT_DIR=$(git rev-parse --show-toplevel)
SPARK_DIR=$ROOT_DIR/spark
SCRIPTS_DIR=$ROOT_DIR/scripts
source ../docker/setup.sh
source $ROOT_DIR/scripts/spark/spark_version
mkdir -p "${SPARK_DIR}/volume/logs"
rm -f "${SPARK_DIR}/volume/logs/hiveserver2*.log"

mkdir -p "${SPARK_DIR}/volume/status"
rm -f "${SPARK_DIR}/volume/status/HIVESERVER2*"

mkdir -p "${SPARK_DIR}/volume/metastore"
mkdir -p "${SPARK_DIR}/volume/user/hive"

# We need to be absolutely sure that ssh configuration exists
mkdir -p ${SPARK_DIR}/volume/ssh
touch ${SPARK_DIR}/volume/ssh/authorized_keys
touch ${SPARK_DIR}/volume/ssh/config

CMD="${DOCKER_HOME_DIR}/bin/start-thrift.sh"

RUNNING_MODE="daemon"
START_LOCAL="YES"
# STORAGE_HOST1="--add-host=qflock-storage-dc1:$($SCRIPTS_DIR/get-docker-ip.py qflock-storage-dc1)"
STORAGE_HOST2="--add-host=qflock-storage-dc2:$($SCRIPTS_DIR/get-docker-ip.py qflock-net-dc2 qflock-storage-dc2)"
LOCAL_DOCKER_HOST="--add-host=local-docker-host:$($SCRIPTS_DIR/get-docker-ip.py qflock-net-dc2 qflock-net-dc2)"

echo "Local docker host ${LOCAL_DOCKER_HOST}"
echo "Storage ${STORAGE_HOST1} ${STORAGE_HOST2}"

DOCKER_ID=""
if [ $RUNNING_MODE = "interactive" ]; then
  DOCKER_IT="-i -t"
fi
echo "Command is: ${CMD}"
DOCKER_NAME="qflock-spark-dc2"
if [ ${START_LOCAL} == "YES" ]; then
  DOCKER_RUN="docker run ${DOCKER_IT} --rm \
  -p 5007:5007 \
  --expose 10001 \
  --name $DOCKER_NAME --hostname $DOCKER_NAME \
  $STORAGE_HOST2 $LOCAL_DOCKER_HOST \
  --network qflock-net-dc2 \
  -e MASTER=spark://sparkmaster:7077 \
  -e SPARK_CONF_DIR=/conf \
  -e SPARK_PUBLIC_DNS=localhost \
  -e SPARK_LOG_DIR=/opt/volume/logs \
  -e HADOOP_CONF_DIR=/opt/spark-$SPARK_VERSION/conf \
  --mount type=bind,source=$ROOT_DIR,target=/qflock \
  -w /qflock/benchmark/src \
  --mount type=bind,source=$SPARK_DIR/spark,target=/spark \
  --mount type=bind,source=$SPARK_DIR/extensions/,target=/extensions \
  -v $ROOT_DIR/conf/spark:/opt/spark-$SPARK_VERSION/conf  \
  -v $ROOT_DIR/conf/hdfs-site.xml:/opt/spark-$SPARK_VERSION/conf/hdfs-site.xml  \
  -v $ROOT_DIR/conf/hive-site.xml:/opt/spark-$SPARK_VERSION/conf/hive-site.xml  \
  -v ${SPARK_DIR}/volume/metastore:/opt/volume/metastore \
  -v ${SPARK_DIR}/volume/user/hive:/user/hive \
  -v ${SPARK_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2 \
  -v ${SPARK_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg \
  -v ${SPARK_DIR}/build/.sbt:${DOCKER_HOME_DIR}/.sbt \
  -v ${SPARK_DIR}/build/.cache:${DOCKER_HOME_DIR}/.cache \
  -v ${SPARK_DIR}/build/.ivy2:${DOCKER_HOME_DIR}/.ivy2 \
  -v ${SPARK_DIR}/volume/status:/opt/volume/status \
  -v ${SPARK_DIR}/volume/logs:/opt/volume/logs \
  -v ${SPARK_DIR}/bin/:${DOCKER_HOME_DIR}/bin \
  -v ${SPARK_DIR}/volume/ssh/authorized_keys:/home/${USER_NAME}/.ssh/authorized_keys \
  -v ${SPARK_DIR}/volume/ssh/config:/home/${USER_NAME}/.ssh/config \
  -e RUNNING_MODE=${RUNNING_MODE} \
  -u ${USER_ID} \
  ${SPARK_DOCKER_NAME} ${CMD}"
else
  DOCKER_RUN="docker run ${DOCKER_IT} --rm \
  -p 5006:5006 \
  --name $DOCKER_NAME --hostname $DOCKER_NAME \
  --network qflock-net --ip ${LAUNCHER_IP} ${DOCKER_HOSTS} \
  -w /qflock/benchmark/src \
  -e MASTER=spark://sparkmaster:7077 \
  -e SPARK_CONF_DIR=/conf \
  -e SPARK_PUBLIC_DNS=localhost \
  -e SPARK_MASTER="spark://sparkmaster:7077" \
  -e SPARK_DRIVER_HOST=${LAUNCHER_IP} \
  --mount type=bind,source=$SPARK_DIR/spark,target=/spark \
  --mount type=bind,source=$SPARK_DIR/build,target=/build \
  -v $SPARK_DIR/conf/master:/conf  \
  -v ${SPARK_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2 \
  -v ${SPARK_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg \
  -v ${SPARK_DIR}/build/.sbt:${DOCKER_HOME_DIR}/.sbt \
  -v ${SPARK_DIR}/build/.cache:${DOCKER_HOME_DIR}/.cache \
  -v ${SPARK_DIR}/build/.ivy2:${DOCKER_HOME_DIR}/.ivy2 \
  -v ${SPARK_DIR}/volume/status:/opt/volume/status \
  -v ${SPARK_DIR}/volume/logs:/opt/volume/logs \
  -v ${SPARK_DIR}/bin/:${DOCKER_HOME_DIR}/bin \
  -v ${SPARK_DIR}/volume/ssh/authorized_keys:/home/${USER_NAME}/.ssh/authorized_keys \
  -v ${SPARK_DIR}/volume/ssh/config:/home/${USER_NAME}/.ssh/config \
  -e RUNNING_MODE=${RUNNING_MODE} \
  -u ${USER_ID} \
  ${SPARK_DOCKER_NAME} ${CMD}"
fi
echo $DOCKER_RUN
echo "mode: $RUNNING_MODE"
if [ $RUNNING_MODE = "interactive" ]; then
  eval "${DOCKER_RUN}"
else
  eval "${DOCKER_RUN}" &
  while [ ! -f "${SPARK_DIR}/volume/status/HIVESERVER2_STATE" ]; do
    echo "looking for ${SPARK_DIR}/volume/status/HIVESERVER2_STATE"
    sleep 1
  done
  cat "${SPARK_DIR}/volume/status/HIVESERVER2_STATE"
fi

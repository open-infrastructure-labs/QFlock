#!/bin/bash

pushd "$(dirname "$0")"
ROOT_DIR=$(git rev-parse --show-toplevel)
SPARK_DIR=$ROOT_DIR/spark
SCRIPTS_DIR=$ROOT_DIR/scripts
# Include the setup for our cached local directories. (.m2, .ivy2, etc)
source ../docker/setup.sh
source $ROOT_DIR/scripts/spark/spark_version
mkdir -p "${SPARK_DIR}/volume/logs"
rm -f "${SPARK_DIR}/volume/logs/master*.log"

mkdir -p "${SPARK_DIR}/volume/status"
rm -f "${SPARK_DIR}/volume/status/MASTER*"

mkdir -p "${SPARK_DIR}/volume/metastore"
mkdir -p "${SPARK_DIR}/volume/user/hive"

# We need to be absolutely sure that ssh configuration exists
mkdir -p ${SPARK_DIR}/volume/ssh
touch ${SPARK_DIR}/volume/ssh/authorized_keys
touch ${SPARK_DIR}/volume/ssh/config

CMD="/qflock/spark/scripts/start_spark_docker.sh"
RUNNING_MODE="daemon"
START_LOCAL="YES"
STORAGE_HOST1="--add-host=qflock-storage-dc1:$($SCRIPTS_DIR/get-docker-ip.py qflock-net-dc1 qflock-storage-dc1)"
# STORAGE_HOST2="--add-host=qflock-storage-dc2:$($SCRIPTS_DIR/get-docker-ip.py qflock-storage-dc2)"
#DC2_SPARK_HOST="--add-host=qflock-spark-dc2:$($SCRIPTS_DIR/get-docker-ip.py qflock-spark-dc2)"
LOCAL_DOCKER_HOST="--add-host=local-docker-host:$($SCRIPTS_DIR/get-docker-ip.py qflock-net-dc1 qflock-net-dc1)"
# JDBC_DOCKER="--add-host=qflock-jdbc-dc2:$($SCRIPTS_DIR/get-docker-ip.py qflock-jdbc-dc2)"

SPARK_RAMDISK=${SPARK_DIR}/spark_rd
if [ ! -d $SPARK_RAMDISK ]; then
    mkdir -p $SPARK_RAMDISK
fi
#sudo mount -t tmpfs -o size=64G tmpfs $SPARK_RAMDISK

echo "Local docker host ${LOCAL_DOCKER_HOST}"
echo "Storage ${STORAGE_HOST1} ${STORAGE_HOST1}"

DOCKER_ID=""
if [ $RUNNING_MODE = "interactive" ]; then
  DOCKER_IT="-i -t"
fi
echo "Command is: ${CMD}"
DOCKER_NAME="qflock-spark-dc1"
if [ ${START_LOCAL} == "YES" ]; then
  DOCKER_RUN="docker run ${DOCKER_IT} --rm \
  -p 5006:5006 \
  --name $DOCKER_NAME --hostname $DOCKER_NAME \
  $STORAGE_HOST1 $LOCAL_DOCKER_HOST \
  --network qflock-net-dc1 \
  -e MASTER=spark://sparkmaster:7077 \
  -e SPARK_CONF_DIR=/conf \
  -e SPARK_PUBLIC_DNS=localhost \
  -e SPARK_LOG_DIR=/opt/volume/logs \
  -e HADOOP_CONF_DIR=/opt/spark-$SPARK_VERSION/conf \
  -w /qflock/benchmark/src \
  --mount type=bind,source=$ROOT_DIR,target=/qflock \
  --mount type=bind,source=$SPARK_DIR/spark,target=/spark \
  --mount type=bind,source=$SPARK_DIR/extensions/,target=/extensions \
  --mount type=bind,source=${SPARK_DIR}/spark_rd,target=/spark_rd \
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
  -v ${SPARK_DIR}/volume/ssh/authorized_keys:/home/${USER_NAME}/.ssh/authorized_keys \
  -v ${SPARK_DIR}/volume/ssh/config:/home/${USER_NAME}/.ssh/config \
  -v ${SPARK_DIR}/bin/:${DOCKER_HOME_DIR}/bin \
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
fi

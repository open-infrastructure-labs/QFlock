#!/bin/bash

set -e # exit on error

if [ ! -f docker/setup.sh ]; then
  echo "please launch script from spark directory"
  exit 1
fi

# Include the setup for our cached local directories. (.m2, .ivy2, etc)
source docker/setup.sh
source docker/spark_version

pushd "$(dirname "$0")" # connect to root
ROOT_DIR=${PWD}/../
echo "ROOT_DIR ${ROOT_DIR}"
popd

mkdir -p "${ROOT_DIR}/volume/logs"
rm -f "${ROOT_DIR}/volume/logs/hiveserver2*.log"

mkdir -p "${ROOT_DIR}/volume/status"
rm -f "${ROOT_DIR}/volume/status/HIVESERVER2*"

mkdir -p "${ROOT_DIR}/volume/metastore"
mkdir -p "${ROOT_DIR}/volume/user/hive"

CMD="${DOCKER_HOME_DIR}/bin/start-thrift.sh"

RUNNING_MODE="daemon"
START_LOCAL="NO"
CONFIG="scripts/conf/spark.config"
if [ ! -f ${CONFIG} ]; then
  START_LOCAL="YES"
else
  DOCKER_HOSTS="$(cat ${CONFIG} | grep DOCKER_HOSTS)"
  IFS='=' read -a IP_ARRAY <<< "$DOCKER_HOSTS"
  DOCKER_HOSTS=${IP_ARRAY[1]}
  HOSTS=""
  IFS=',' read -a IP_ARRAY <<< "$DOCKER_HOSTS"
  for i in "${IP_ARRAY[@]}"
  do
    HOSTS="$HOSTS --add-host=$i"
  done
  DOCKER_HOSTS=$HOSTS
  echo "Docker Hosts: $DOCKER_HOSTS"

  LAUNCHER_IP="$(cat ${CONFIG} | grep LAUNCHER_IP)"
  IFS='=' read -a IP_ARRAY <<< "$LAUNCHER_IP"
  LAUNCHER_IP=${IP_ARRAY[1]}
  echo "LAUNCHER_IP: $LAUNCHER_IP"
fi
START_LOCAL="YES"
STORAGE_HOST2="--add-host=qflock-storage-dc2:$(scripts/get-docker-ip.py qflock-net-dc2 qflock-storage-dc2)"
LOCAL_DOCKER_HOST="--add-host=local-docker-host:$(scripts/get-docker-ip.py qflock-net qflock-net)"

echo "Local docker host ${LOCAL_DOCKER_HOST}"
echo "Storage ${STORAGE_HOST2}"

DOCKER_ID=""
if [ $RUNNING_MODE = "interactive" ]; then
  DOCKER_IT="-i -t"
fi
echo "Command is: ${CMD}"
if [ ${START_LOCAL} == "YES" ]; then
  DOCKER_RUN="docker run ${DOCKER_IT} --rm \
  -p 5007:5007 \
  --expose 10001 \
  --name qflock-spark-dc2 ${STORAGE_HOST2} ${LOCAL_DOCKER_HOST} \
  --network qflock-net-dc2 \
  -e MASTER=spark://sparkmaster:7077 \
  -e SPARK_CONF_DIR=/conf \
  -e SPARK_PUBLIC_DNS=localhost \
  -e SPARK_LOG_DIR=/opt/volume/logs \
  --mount type=bind,source=$(pwd)/../,target=/qflock \
  -w /qflock/benchmark/src \
  --mount type=bind,source=$(pwd)/spark,target=/spark \
  --mount type=bind,source=$(pwd)/extensions/,target=/extensions \
  -v $(pwd)/conf/hiveserver2:/opt/spark-$SPARK_VERSION/conf  \
  -v ${ROOT_DIR}/volume/metastore:/opt/volume/metastore \
  -v ${ROOT_DIR}/volume/user/hive:/user/hive \
  -v ${ROOT_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2 \
  -v ${ROOT_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg \
  -v ${ROOT_DIR}/build/.sbt:${DOCKER_HOME_DIR}/.sbt \
  -v ${ROOT_DIR}/build/.cache:${DOCKER_HOME_DIR}/.cache \
  -v ${ROOT_DIR}/build/.ivy2:${DOCKER_HOME_DIR}/.ivy2 \
  -v ${ROOT_DIR}/volume/status:/opt/volume/status \
  -v ${ROOT_DIR}/volume/logs:/opt/volume/logs \
  -v ${ROOT_DIR}/bin/:${DOCKER_HOME_DIR}/bin \
  -e RUNNING_MODE=${RUNNING_MODE} \
  -u ${USER_ID} \
  ${SPARK_DOCKER_NAME} ${CMD}"
else
  DOCKER_RUN="docker run ${DOCKER_IT} --rm \
  -p 5006:5006 \
  --name qflock-spark-dc2 \
  --network qflock-net-dc2 --ip ${LAUNCHER_IP} ${DOCKER_HOSTS} \
  -w /qflock/benchmark/src \
  -e MASTER=spark://sparkmaster:7077 \
  -e SPARK_CONF_DIR=/conf \
  -e SPARK_PUBLIC_DNS=localhost \
  -e SPARK_MASTER="spark://sparkmaster:7077" \
  -e SPARK_DRIVER_HOST=${LAUNCHER_IP} \
  --mount type=bind,source=$(pwd)/spark,target=/spark \
  --mount type=bind,source=$(pwd)/build,target=/build \
  -v $(pwd)/conf/master:/conf  \
  -v ${ROOT_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2 \
  -v ${ROOT_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg \
  -v ${ROOT_DIR}/build/.sbt:${DOCKER_HOME_DIR}/.sbt \
  -v ${ROOT_DIR}/build/.cache:${DOCKER_HOME_DIR}/.cache \
  -v ${ROOT_DIR}/build/.ivy2:${DOCKER_HOME_DIR}/.ivy2 \
  -v ${ROOT_DIR}/volume/status:/opt/volume/status \
  -v ${ROOT_DIR}/volume/logs:/opt/volume/logs \
  -v ${ROOT_DIR}/bin/:${DOCKER_HOME_DIR}/bin \
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
  while [ ! -f "${ROOT_DIR}/volume/status/HIVESERVER2_STATE" ]; do
    sleep 1
  done

  cat "${ROOT_DIR}/volume/status/HIVESERVER2_STATE"
fi

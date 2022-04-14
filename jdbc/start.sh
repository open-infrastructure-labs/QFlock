#!/bin/bash

set -e
source docker/setup.sh

pushd "$(dirname "$0")" # connect to root
ROOT_DIR=$(pwd)

mkdir -p "${ROOT_DIR}/volume/logs"
rm -f "${ROOT_DIR}/volume/logs/*.log"

mkdir -p "${ROOT_DIR}/volume/status"
rm -f "${ROOT_DIR}/volume/status/jdbc*"

RUNNING_MODE="daemon"
START_LOCAL="YES"
# STORAGE_HOST1="--add-host=qflock-storage-dc1:$(scripts/get-docker-ip.py qflock-net-dc1 qflock-storage-dc1)"
STORAGE_HOST2="--add-host=qflock-storage-dc2:$(scripts/get-docker-ip.py qflock-net-dc2 qflock-storage-dc2)"
#DC2_SPARK_HOST="--add-host=qflock-dc2-spark:$(scripts/get-docker-ip.py qflock-dc2-spark)"
LOCAL_DOCKER_HOST="--add-host=local-docker-host:$(scripts/get-docker-ip.py qflock-net-dc2 qflock-net-dc2)"

echo "Local docker host ${LOCAL_DOCKER_HOST}"
echo "Storage ${STORAGE_HOST2}"

DOCKER_ID=""
if [ $RUNNING_MODE = "interactive" ]; then
  DOCKER_IT="-i -t"
fi
DOCKER_NAME="qflock-jdbc-dc2"
CMD="/scripts/start-jdbc-daemon.sh"
DOCKER_RUN="docker run ${DOCKER_IT} --rm \
  --name $DOCKER_NAME --hostname $DOCKER_NAME \
  $STORAGE_HOST2 $LOCAL_DOCKER_HOST\
  --network qflock-net-dc2 \
  -e MASTER=spark://sparkmaster:7077 \
  -e SPARK_CONF_DIR=/conf \
  -e SPARK_PUBLIC_DNS=localhost \
  -e SPARK_LOG_DIR=/opt/volume/logs \
  --mount type=bind,source=$(pwd)/../,target=/qflock \
  --mount type=bind,source=$(pwd)/,target=/jdbc \
  --mount type=bind,source=$(pwd)/scripts,target=/scripts \
  -w /jdbc/server \
  -v $(pwd)/conf/master:/opt/spark-$SPARK_VERSION/conf  \
  -v ${ROOT_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2 \
  -v ${ROOT_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg \
  -v ${ROOT_DIR}/build/.sbt:${DOCKER_HOME_DIR}/.sbt \
  -v ${ROOT_DIR}/build/.cache:${DOCKER_HOME_DIR}/.cache \
  -v ${ROOT_DIR}/build/.ivy2:${DOCKER_HOME_DIR}/.ivy2 \
  -v ${ROOT_DIR}/volume/status:/opt/volume/status \
  -v ${ROOT_DIR}/volume/logs:/opt/volume/logs \
  -e RUNNING_MODE=${RUNNING_MODE} \
  -u ${USER_ID} \
  ${JDBC_DOCKER_NAME} ${CMD}"

echo $DOCKER_RUN
echo "mode: $RUNNING_MODE"

if [ $RUNNING_MODE = "interactive" ]; then
  eval "${DOCKER_RUN}"
else
  eval "${DOCKER_RUN}" &
  while [ ! -f "${ROOT_DIR}/volume/status/JDBC_STATE" ]; do
    sleep 1
  done

  cat "${ROOT_DIR}/volume/status/JDBC_STATE"
fi

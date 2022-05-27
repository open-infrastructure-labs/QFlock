#!/bin/bash
set -e
pushd "$(dirname "$0")"
ROOT_DIR=$(git rev-parse --show-toplevel)
source $ROOT_DIR/scripts/spark/spark_version
source ../docker/setup.sh
WORKING_DIR=$(pwd)
echo "WORKING_DIR: $WORKING_DIR"

if [ ! -d build ]; then
  # Create build directories.
  source ../docker/setup_build.sh
fi

SPARK_JAR_DIR=../build/spark-${SPARK_VERSION}/jars/
if [ ! -d $SPARK_JAR_DIR ]; then
  echo "Please build spark before building extensions"
  exit 1
fi
JDBC_JAR_DIR=$ROOT_DIR/jdbc/driver/target/
if [ ! -d $JDBC_JAR_DIR ]; then
  echo "Please build jdbc driver ($JDBC_JAR_DIR) before building extensions"
  exit 1
fi
if [ ! -d ./lib ]; then
  mkdir ./lib
fi
if [ ! -d build ]; then
  mkdir build
fi
echo "Copy over spark jars ($SPARK_JAR_DIR)"
cp $SPARK_JAR_DIR/*.jar ./lib
echo "Copy over jdbc jars ($JDBC_JAR_DIR)"
cp $JDBC_JAR_DIR/*.jar ./lib

if [ "$#" -gt 0 ]; then
  if [ "$1" == "-d" ]; then
    shift
    docker run --rm -it --name extensions-build-debug \
      --network host \
      --mount type=bind,source="$(pwd)",target=/extensions \
      -v "${WORKING_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2" \
      -v "${WORKING_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg" \
      -v "${WORKING_DIR}/build/.sbt:${DOCKER_HOME_DIR}/.sbt" \
      -v "${WORKING_DIR}/build/.cache:${DOCKER_HOME_DIR}/.cache" \
      -v "${WORKING_DIR}/build/.ivy2:${DOCKER_HOME_DIR}/.ivy2" \
      -u "${USER_ID}" \
      --entrypoint /bin/bash -w /extensions \
      ${SPARK_DOCKER_NAME}
  fi
else
  echo "Building extensions"
  docker run --rm -it --name extensions-build \
    --network qflock-net \
    --mount type=bind,source="$(pwd)",target=/extensions \
    -v "${WORKING_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2" \
    -v "${WORKING_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg" \
    -v "${WORKING_DIR}/build/.sbt:${DOCKER_HOME_DIR}/.sbt" \
    -v "${WORKING_DIR}/build/.cache:${DOCKER_HOME_DIR}/.cache" \
    -v "${WORKING_DIR}/build/.ivy2:${DOCKER_HOME_DIR}/.ivy2" \
    -u "${USER_ID}" \
    --entrypoint /extensions/scripts/build.sh -w /extensions \
    ${SPARK_DOCKER_NAME}
fi
popd
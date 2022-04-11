#!/bin/bash
set -e
pushd "$(dirname "$0")" # connect to root
ROOT_DIR=$(pwd)

source ../../spark/docker/spark_version
source ../../spark/docker/setup.sh

if [ ! -d build ]; then
  mkdir build
fi

if [ "$#" -gt 0 ]; then
  if [ "$1" == "-d" ]; then
    shift
    docker run --rm -it --name jdbc-build-debug \
      --network host \
      --mount type=bind,source="$(pwd)",target=/driver \
      -v "${ROOT_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2" \
      -v "${ROOT_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg" \
      -v "${ROOT_DIR}/build/.sbt:${DOCKER_HOME_DIR}/.sbt" \
      -v "${ROOT_DIR}/build/.cache:${DOCKER_HOME_DIR}/.cache" \
      -v "${ROOT_DIR}/build/.ivy2:${DOCKER_HOME_DIR}/.ivy2" \
      -u "${USER_ID}" \
      --entrypoint /bin/bash -w /driver \
      ${SPARK_DOCKER_NAME}
  fi
else
  echo "Building extensions"
  docker run --rm -it --name jdbc-build \
    --network qflock-net \
    --mount type=bind,source="$(pwd)",target=/driver \
    -v "${ROOT_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2" \
    -v "${ROOT_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg" \
    -v "${ROOT_DIR}/build/.sbt:${DOCKER_HOME_DIR}/.sbt" \
    -v "${ROOT_DIR}/build/.cache:${DOCKER_HOME_DIR}/.cache" \
    -v "${ROOT_DIR}/build/.ivy2:${DOCKER_HOME_DIR}/.ivy2" \
    -u "${USER_ID}" \
    --entrypoint /driver/scripts/build.sh -w /driver\
    ${SPARK_DOCKER_NAME}
fi

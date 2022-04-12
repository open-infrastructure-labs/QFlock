#!/bin/bash
set -e
pushd "$(dirname "$0")" # connect to root
ROOT_DIR=$(pwd)

source ../docker/setup.sh

if [ "$#" -gt 0 ]; then
  if [ "$1" == "-d" ]; then
    shift
    docker run --rm -it --name jdbc-build-debug \
      --network qflock-net \
      --mount type=bind,source="${ROOT_DIR}/../",target=/jdbc \
      -v "${ROOT_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2" \
      -v "${ROOT_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg" \
      -v "${ROOT_DIR}/build/.sbt:${DOCKER_HOME_DIR}/.sbt" \
      -v "${ROOT_DIR}/build/.cache:${DOCKER_HOME_DIR}/.cache" \
      -v "${ROOT_DIR}/build/.ivy2:${DOCKER_HOME_DIR}/.ivy2" \
      -u "${USER_ID}" \
      --entrypoint /bin/bash -w /jdbc \
      ${JDBC_DOCKER_NAME}
  fi
else
  echo "generating thrift api"
  docker run --rm -it --name jdbc-build \
    --network qflock-net \
    --mount type=bind,source="${ROOT_DIR}/../",target=/jdbc \
    -v "${ROOT_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2" \
    -v "${ROOT_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg" \
    -v "${ROOT_DIR}/build/.sbt:${DOCKER_HOME_DIR}/.sbt" \
    -v "${ROOT_DIR}/build/.cache:${DOCKER_HOME_DIR}/.cache" \
    -v "${ROOT_DIR}/build/.ivy2:${DOCKER_HOME_DIR}/.ivy2" \
    -u "${USER_ID}" \
    --entrypoint /jdbc/thrift/gen.sh -w /jdbc/thrift \
    ${JDBC_DOCKER_NAME}
fi

source ../spark/docker/spark_version
source ../spark/docker/setup.sh

SPARK_JAR_DIR=../spark/build/spark-${SPARK_VERSION}/jars/
if [ ! -d $SPARK_JAR_DIR ]; then
  echo "Please build spark before building datasource"
  exit 1
fi
if [ ! -d ./lib ]; then
  mkdir ./lib
fi
if [ ! -d build ]; then
  mkdir build
fi
echo "Copy over spark jars"
cp $SPARK_JAR_DIR/*.jar ./lib

#SPARK_TEST_JAR_DIR=../spark/spark/
#cp $SPARK_TEST_JAR_DIR/sql/core/target/spark-sql_2.12-${SPARK_VERSION}-tests.jar ./lib
#cp $SPARK_TEST_JAR_DIR/sql/catalyst/target/spark-catalyst_2.12-${SPARK_VERSION}-tests.jar ./lib
#cp $SPARK_TEST_JAR_DIR/core/target/spark-core_2.12-${SPARK_VERSION}-tests.jar ./lib

if [ "$#" -gt 0 ]; then
  if [ "$1" == "-d" ]; then
    shift
    docker run --rm -it --name datasource-build-debug \
      --network host \
      --mount type=bind,source="$(pwd)",target=/datasource \
      -v "${ROOT_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2" \
      -v "${ROOT_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg" \
      -v "${ROOT_DIR}/build/.sbt:${DOCKER_HOME_DIR}/.sbt" \
      -v "${ROOT_DIR}/build/.cache:${DOCKER_HOME_DIR}/.cache" \
      -v "${ROOT_DIR}/build/.ivy2:${DOCKER_HOME_DIR}/.ivy2" \
      -u "${USER_ID}" \
      --entrypoint /bin/bash -w /datasource \
      v${QFLOCK_VERSION}-spark-${USER_NAME}
  fi
else
  echo "Building datasource"
  docker run --rm -it --name pushdown_datasource_build \
    --network dike-net \
    --mount type=bind,source="$(pwd)",target=/datasource \
    -v "${ROOT_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2" \
    -v "${ROOT_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg" \
    -v "${ROOT_DIR}/build/.sbt:${DOCKER_HOME_DIR}/.sbt" \
    -v "${ROOT_DIR}/build/.cache:${DOCKER_HOME_DIR}/.cache" \
    -v "${ROOT_DIR}/build/.ivy2:${DOCKER_HOME_DIR}/.ivy2" \
    -u "${USER_ID}" \
    --entrypoint /datasource/scripts/build.sh -w /datasource \
    v${QFLOCK_VERSION}-spark-${USER_NAME}
fi

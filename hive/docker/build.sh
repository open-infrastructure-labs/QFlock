#!/usr/bin/env bash

set -e               # exit on error
pushd "$(dirname "$0")" # connect to root

ROOT_DIR=$(pwd)

DOCKER_DIR=${ROOT_DIR}
DOCKER_FILE="${DOCKER_DIR}/Dockerfile"
DOCKER_NAME=qflock-hive-2.3.8

# Download Hadoop
ENV_HADOOP_VERSION=2.7.2
if [ ! -f ${DOCKER_DIR}/hadoop-${ENV_HADOOP_VERSION}.tar.gz ]
then
  echo "Downloading hadoop-${ENV_HADOOP_VERSION}.tar.gz"
  curl -L https://archive.apache.org/dist/hadoop/common/hadoop-${ENV_HADOOP_VERSION}/hadoop-${ENV_HADOOP_VERSION}.tar.gz  --output ${DOCKER_DIR}/hadoop-${ENV_HADOOP_VERSION}.tar.gz
fi

# Download HIVE
ENV_HIVE_VERSION=2.3.8
if [ ! -f ${DOCKER_DIR}/apache-hive-${ENV_HIVE_VERSION}-bin.tar.gz ]
then
  echo "Downloading apache-hive-${ENV_HIVE_VERSION}-bin.tar.gz"
  curl -L https://archive.apache.org/dist/hive/hive-${ENV_HIVE_VERSION}//apache-hive-${ENV_HIVE_VERSION}-bin.tar.gz  --output ${DOCKER_DIR}/apache-hive-${ENV_HIVE_VERSION}-bin.tar.gz
fi

# Download Tez
ENV_TEZ_VERSION=0.9.2
#if [ ! -f ${DOCKER_DIR}/apache-tez-${ENV_TEZ_VERSION}-bin.tar.gz ]
#then
#  echo "Downloading apache-tez-${ENV_TEZ_VERSION}-bin.tar.gz"
#  curl -L https://downloads.apache.org/tez/${ENV_TEZ_VERSION}/apache-tez-${ENV_TEZ_VERSION}-bin.tar.gz  --output ${DOCKER_DIR}/apache-tez-${ENV_TEZ_VERSION}-bin.tar.gz
#fi
rm -rf tez
echo "Download tez source code and switch to branch-0.9.2"
git clone https://github.com/apache/tez.git
cd tez
git switch branch-0.9.2
git apply ../qflock-tez.patch

# Download protobuf 2.5.0 required by tez
ENV_PROTOBUF_VERSION=2.5.0
if [ ! -f ${DOCKER_DIR}/protobuf-${ENV_PROTOBUF_VERSION}.tar.gz ]
then
  echo "Downloading protobuf-${ENV_PROTOBUF_VERSION}.tar.gz"
  curl -L https://github.com/google/protobuf/releases/download/v${ENV_PROTOBUF_VERSION}/protobuf-${ENV_PROTOBUF_VERSION}.tar.gz --output ${DOCKER_DIR}/protobuf-${ENV_PROTOBUF_VERSION}.tar.gz
fi

DOCKER_CMD="docker build -t ${DOCKER_NAME} --build-arg HADOOP_VERSION -f $DOCKER_FILE $DOCKER_DIR"
eval "$DOCKER_CMD"

USER_NAME=${SUDO_USER:=$USER}
USER_ID=$(id -u "${USER_NAME}")
GROUP_ID=$(id -g "${USER_NAME}")

# Set the home directory in the Docker container.
DOCKER_HOME_DIR=${DOCKER_HOME_DIR:-/home/${USER_NAME}}

docker build -t "${DOCKER_NAME}-${USER_NAME}" - <<UserSpecificDocker
FROM ${DOCKER_NAME}
RUN rm -f /var/log/faillog /var/log/lastlog
RUN groupadd --non-unique -g ${GROUP_ID} ${USER_NAME}
RUN useradd -g ${GROUP_ID} -u ${USER_ID} -k /root -m ${USER_NAME} -d "${DOCKER_HOME_DIR}"
RUN echo "${USER_NAME} ALL=NOPASSWD: ALL" >> "/etc/sudoers"
ENV HOME "${DOCKER_HOME_DIR}"

USER ${USER_NAME}
WORKDIR "${DOCKER_HOME_DIR}"
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
RUN cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
RUN chmod 0600 ~/.ssh/authorized_keys
UserSpecificDocker

popd

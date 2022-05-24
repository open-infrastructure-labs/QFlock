#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -e
pushd "$(dirname "$0")"
source setup.sh

DOCKER_DIR=$(pwd)
DOCKER_FILE="${DOCKER_DIR}/Dockerfile"

THRIFT_VERSION=0.16.0
THRIFT_PACKAGE=thrift-0.16.0.tar.gz
THRIFT_PACKAGE_URL=https://dlcdn.apache.org/thrift/0.16.0/thrift-0.16.0.tar.gz
if [ ! -f ${DOCKER_DIR}/${THRIFT_PACKAGE} ]
then
  echo "build.sh: Downloading ${THRIFT_PACKAGE}"
  curl -L ${THRIFT_PACKAGE_URL} --output ${DOCKER_DIR}/${THRIFT_PACKAGE}
fi
if [ ! -f ${DOCKER_DIR}/${THRIFT_PACKAGE} ]
then
  exit
fi

echo "build.sh: Building ${JDBC_DOCKER_BASE_NAME} docker"
docker build -f Dockerfile --build-arg BASE_IMAGE=$SPARK_DOCKER_BASE_NAME \
                           --build-arg THRIFT_VERSION=$THRIFT_VERSION \
                           -t ${JDBC_DOCKER_BASE_NAME} -f Dockerfile .
STATUS=$?
if [ $STATUS -ne 0 ]; then
    echo "build.sh: Error building docker (status=$STATUS)"
    exit
else
    echo "build.sh: Successfully built ${JDBC_DOCKER_BASE_NAME} docker"
fi

USER_NAME=${SUDO_USER:=$USER}
USER_ID=$(id -u "${USER_NAME}")
GROUP_ID=$(id -g "${USER_NAME}")

echo "User id is: $USER_ID"
echo "Group id is: $GROUP_ID"

# Set the home directory in the Docker container.
DOCKER_HOME_DIR=${DOCKER_HOME_DIR:-/home/${USER_NAME}}

docker build -t "${JDBC_DOCKER_NAME}" - <<UserSpecificDocker
FROM ${JDBC_DOCKER_BASE_NAME}
RUN rm -f /var/log/faillog /var/log/lastlog
RUN groupadd --non-unique -g ${GROUP_ID} ${USER_NAME}
RUN useradd -g ${GROUP_ID} -u ${USER_ID} -k /root -m ${USER_NAME} -d "${DOCKER_HOME_DIR}"
RUN echo "${USER_NAME} ALL=NOPASSWD: ALL" > "/etc/sudoers.d/jdbc-build-${USER_ID}"
ENV HOME "${DOCKER_HOME_DIR}"

USER ${USER_NAME}
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
RUN cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
RUN chmod 0600 ~/.ssh/authorized_keys

EXPOSE 22

UserSpecificDocker

STATUS=$?
if [ $STATUS -ne 0 ]; then
    echo "build.sh: Error building docker ${JDBC_DOCKER_NAME} (status=$STATUS)"
    exit 1
else
    echo "build.sh: Successfully built docker ${JDBC_DOCKER_NAME}"
    exit 0
fi

popd

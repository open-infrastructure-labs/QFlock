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
pushd "$(dirname "$0")"
source spark_version
echo "build.sh: SPARK_VERSION $SPARK_VERSION"
source setup.sh

DOCKER_DIR=$(pwd)
DOCKER_FILE="${DOCKER_DIR}/Dockerfile"

if [ ! -f ${DOCKER_DIR}/${SPARK_PACKAGE} ]
then
  echo "build.sh: Downloading ${SPARK_PACKAGE}"
  curl -L ${SPARK_PACKAGE_URL} --output ${DOCKER_DIR}/${SPARK_PACKAGE}
fi
if [ ! -f ${DOCKER_DIR}/${SPARK_PACKAGE} ]
then
  exit
fi
if [ ! -f ${DOCKER_DIR}/${HADOOP_PACKAGE} ]
then
  echo "build.sh: Downloading ${HADOOP_PACKAGE}"
  curl -L ${HADOOP_PACKAGE_URL} --output ${DOCKER_DIR}/${HADOOP_PACKAGE}
fi
if [ ! -f ${DOCKER_DIR}/${HADOOP_PACKAGE} ]
then
  exit
fi

echo "build.sh: Building ${SPARK_DOCKER_BASE_NAME} docker"
docker build -f Dockerfile --build-arg SPARK_VERSION=$SPARK_VERSION \
                           --build-arg HADOOP_VERSION=$HADOOP_VERSION \
                           -t ${SPARK_DOCKER_BASE_NAME} -f Dockerfile .
STATUS=$?
if [ $STATUS -ne 0 ]; then
    echo "build.sh: Error building docker (status=$STATUS)"
    exit
else
    echo "build.sh: Successfully built ${SPARK_DOCKER_BASE_NAME} docker"
fi

USER_NAME=${SUDO_USER:=$USER}
USER_ID=$(id -u "${USER_NAME}")
GROUP_ID=$(id -g "${USER_NAME}")

echo "User id is: $USER_ID"
echo "Group id is: $GROUP_ID"

# Set the home directory in the Docker container.
DOCKER_HOME_DIR=${DOCKER_HOME_DIR:-/home/${USER_NAME}}

docker build -t "${SPARK_DOCKER_NAME}" - <<UserSpecificDocker
FROM ${SPARK_DOCKER_BASE_NAME}
RUN rm -f /var/log/faillog /var/log/lastlog
RUN groupadd --non-unique -g ${GROUP_ID} ${USER_NAME}
RUN useradd -g ${GROUP_ID} -u ${USER_ID} -k /root -m ${USER_NAME} -d "${DOCKER_HOME_DIR}"
RUN echo "${USER_NAME} ALL=NOPASSWD: ALL" > "/etc/sudoers.d/spark-build-${USER_ID}"
ENV HOME "${DOCKER_HOME_DIR}"

USER ${USER_NAME}
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
RUN cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
RUN chmod 0600 ~/.ssh/authorized_keys

EXPOSE 22

UserSpecificDocker

STATUS=$?
if [ $STATUS -ne 0 ]; then
    echo "build.sh: Error building docker ${SPARK_DOCKER_NAME} (status=$STATUS)"
    exit 1
else
    echo "build.sh: Successfully built docker ${SPARK_DOCKER_NAME}"
    exit 0
fi

popd
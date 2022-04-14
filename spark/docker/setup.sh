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
WORKING_DIR=$(pwd)
ROOT_DIR=$(git rev-parse --show-toplevel)
echo "ROOT_DIR $ROOT_DIR"
echo "WORKING_DIR $WORKING_DIR"

QFLOCK_VERSION=$(cat $ROOT_DIR/qflock_version)
echo "QFLOCK VERSION: ${QFLOCK_VERSION}"

HADOOP_VERSION="2.7.4"
HADOOP_PACKAGE_URL="https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"
HADOOP_PACKAGE="hadoop-${HADOOP_VERSION}.tar.gz"
HADOOP_DIR="hadoop-${HADOOP_VERSION}"
USER_NAME=${SUDO_USER:=$USER}
USER_ID=$(id -u "${USER_NAME}")

SPARK_DOCKER_BASE_NAME="v${QFLOCK_VERSION}-qflock-spark"
SPARK_DOCKER_NAME="${SPARK_DOCKER_BASE_NAME}-${USER_NAME}"

# Set the home directory in the Docker container.
DOCKER_HOME_DIR=${DOCKER_HOME_DIR:-/home/${USER_NAME}}

#If this env variable is empty, docker will be started
# in non interactive mode
DOCKER_INTERACTIVE_RUN=${DOCKER_INTERACTIVE_RUN-"-i -t"}

echo "Successfully included setup.sh"
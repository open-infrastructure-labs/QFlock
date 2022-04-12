# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# the base image name is passed as an argument since it contains
# a version and is dynamic.
# In general we base our image on a spark image since spark is a
# dependency.
ARG BASE_IMAGE=
FROM $BASE_IMAGE
LABEL version="1.0"
LABEL maintainer="Robert Foley <robfoley972@gmail.com>"

# JAVA
RUN apt-get update \
 && apt install openjdk-8-jdk scala git maven ant ant-optional sudo -y \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s /usr/lib/jvm/java-8-openjdk-amd64/bin /usr/lib/jvm/bin \
 && update-java-alternatives --set /usr/lib/jvm/java-1.8.0-openjdk-amd64

# Setup Thrift Environment
ARG THRIFT_VERSION=
ENV THRIFT_PACKAGE thrift-${THRIFT_VERSION}.tar.gz
ENV THRIFT_PATH=/opt/thrift-${THRIFT_VERSION}
RUN echo "$THRIFT_VERSION $THRIFT_PATH $THRIFT_PACKAGE"

COPY /${THRIFT_PACKAGE} /opt
RUN cd /opt && tar -xzf ${THRIFT_PACKAGE} && \
    cd ${THRIFT_PATH} && pwd && ls -l && \
  ./configure --with-java --with-python --host=x86_64 --target=x86_64 --build=x86_64\
  make && \
  make install && \
  rm /opt/${THRIFT_PACKAGE}

COPY entry.sh /usr/local/bin/entry.sh
RUN chmod a+x /usr/local/bin/entry.sh

ENTRYPOINT ["/usr/local/bin/entry.sh"]


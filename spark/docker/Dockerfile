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

FROM ubuntu:20.04
LABEL version="1.0"
LABEL maintainer="Robert Foley <robfoley972@gmail.com>"

RUN apt-get update \
 && apt-get install -y locales sudo\
 && dpkg-reconfigure -f noninteractive locales \
 && locale-gen en_US.UTF-8 \
 && /usr/sbin/update-locale LANG=en_US.UTF-8 \
 && echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen \
 && locale-gen \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Users with other locales should set this in their derivative image
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y tzdata \
 && apt-get install -y wget curl unzip nano sudo vim htop iproute2 iputils-ping \
   python3 python3-setuptools python3.8-venv\
 && ln -s /usr/bin/python3 /usr/bin/python \
 && apt install -y python3-pip \
 && python3 -mpip install py4j pyarrow pandas pyfiglet pyyaml hive-metastore-client docker\
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# http://blog.stuart.axelbrooke.com/python-3-on-spark-return-of-the-pythonhashseed
ENV PYTHONHASHSEED 0
ENV PYTHONIOENCODING UTF-8
ENV PIP_DISABLE_PIP_VERSION_CHECK 1

# JAVA
RUN apt-get update \
 && apt install openjdk-8-jdk scala git maven sudo -y \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s /usr/lib/jvm/java-8-openjdk-amd64/bin /usr/lib/jvm/bin \
 && update-java-alternatives --set /usr/lib/jvm/java-1.8.0-openjdk-amd64

# sbt
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list \
&& echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list \
&& curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add  \
&& sudo apt-get update \
&& sudo apt-get install sbt

# Setup Hadoop
ARG HADOOP_VERSION=
ENV HADOOP_VERSION ${HADOOP_VERSION}
ENV HADOOP_PACKAGE hadoop-${HADOOP_VERSION}.tar.gz
ADD /${HADOOP_PACKAGE} /opt
ENV HADOOP_HOME /opt/hadoop-${HADOOP_VERSION}
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:$HADOOP_HOME/lib/native
RUN echo $LD_LIBRARY_PATH $HADOOP_HOME
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

# Setup Spark Environment
ARG SPARK_VERSION=
ENV SPARK_VERSION ${SPARK_VERSION}
ENV SPARK_PACKAGE spark-${SPARK_VERSION}-bin-hadoop2.7.tgz
ENV SPARK_PACKAGE_FOLDER spark-${SPARK_VERSION}-bin-hadoop2.7
ENV SPARK_PACKAGE_URL https://downloads.apache.org/spark/spark-${SPARK_VERSION}/$SPARK_PACKAGE
ENV SPARK_SRC /spark
ENV SPARK_BUILD /build
ENV SPARK_HOME /opt/spark-${SPARK_VERSION}
ENV SPARK_CONF_DIR /opt/spark-$SPARK_VERSION/conf/
ENV PATH $PATH:${SPARK_HOME}/bin
ENV echo "export SPARK_HOME=/opt/spark" >> ~/.profile
ENV echo "export SPARK_VERSION=$SPARK_VERSION" >> ~/.profile
ENV echo "export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin" >> ~/.profile
ENV echo "export PYSPARK_PYTHON=/usr/bin/python3" >> ~/.profile

ADD /${SPARK_PACKAGE} /opt
RUN mv /opt/${SPARK_PACKAGE_FOLDER} ${SPARK_HOME}
RUN sudo python3 -mpip install pyspark==${SPARK_VERSION}
WORKDIR $SPARK_HOME

# Install iperf3 for bandwidth testing
RUN apt-get -q update && apt-get -q install -y --no-install-recommends iperf3

COPY entry.sh /usr/local/bin/entry.sh
RUN chmod a+x /usr/local/bin/entry.sh

ENTRYPOINT ["/usr/local/bin/entry.sh"]


FROM ubuntu:focal

WORKDIR /root
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

#####
# Disable suggests/recommends
#####
RUN echo APT::Install-Recommends "0"\; > /etc/apt/apt.conf.d/10disableextras
RUN echo APT::Install-Suggests "0"\; >>  /etc/apt/apt.conf.d/10disableextras

ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_TERSE true
ENV TZ=US/Eastern

# Install Hadoop dependencies
RUN apt-get -q update \
    && apt-get -q install -y --no-install-recommends \
    openjdk-8-jdk \
    net-tools \
    curl \
    netcat

ENV ENV_HADOOP_VERSION=3.1.3
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

RUN mkdir -p /opt/hadoop
# RUN curl -L https://downloads.apache.org/hadoop/common/hadoop-${ENV_HADOOP_VERSION}/hadoop-${ENV_HADOOP_VERSION}.tar.gz | tar -xz -C /opt/hadoop
ADD /hadoop-${ENV_HADOOP_VERSION}.tar.gz /opt/hadoop

# https://dlcdn.apache.org/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz
# https://downloads.apache.org/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz
# install hive 3.1.2 compiled from source - resolve guava version issue
RUN mkdir -p /opt/hive
ADD /apache-hive-3.1.2-bin.tar.gz /opt/hive

# Install tez-0.10.1 compiled from source
RUN mkdir -p /opt/tez/apache-tez-0.10.1-bin
ADD /tez-0.10.1.tar.gz /opt/tez/apache-tez-0.10.1-bin

# Fix multiple SLF4J binding conflict.
ENV TEZ_HOME=/opt/tez/apache-tez-0.10.1-bin
ENV HIVE_HOME=/opt/hive/apache-hive-3.1.2-bin
RUN rm ${TEZ_HOME}/lib/slf4j-log4j12-1.7.30.jar
RUN rm ${HIVE_HOME}/lib/log4j-slf4j-impl-2.10.0.jar

RUN apt-get -q update \
    && apt-get -q install -y --no-install-recommends \
        keychain  ssh  openssh-server sudo

# Install python and packages dependencies
RUN apt-get -q update \
    && apt-get -q install -y --no-install-recommends \
        python3-pip python3-venv python3-wheel

RUN pip3 install pandas sqlparse fastparquet pyarrow thrift

# install build essential for compiling within docker
RUN apt-get -q update \
    && apt-get -q install -y build-essential

# Install protobuf 2.5.0 for tez
ADD /protobuf-2.5.0.tar.gz /tmp
RUN cd /tmp/protobuf-2.5.0 \
    && ./configure --prefix=/usr --disable-shared \
    && make && make install

COPY entry.sh /usr/local/bin/entry.sh
RUN chmod a+x /usr/local/bin/entry.sh

ENTRYPOINT ["/usr/local/bin/entry.sh"]

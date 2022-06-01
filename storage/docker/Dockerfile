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

RUN mkdir -p /opt/hadoop
ENV ENV_HADOOP_VERSION=3.3.0
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

RUN mkdir -p /opt/hadoop
# RUN curl -L https://downloads.apache.org/hadoop/common/hadoop-${ENV_HADOOP_VERSION}/hadoop-${ENV_HADOOP_VERSION}.tar.gz | tar -xz -C /opt/hadoop
ADD /hadoop-${ENV_HADOOP_VERSION}.tar.gz /opt/hadoop

# https://dlcdn.apache.org/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz
# https://downloads.apache.org/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz
RUN mkdir -p /opt/hive
# RUN curl -L https://downloads.apache.org/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz | tar -xz -C /opt/hive
ADD /apache-hive-3.1.2-bin.tar.gz /opt/hive

# Fix guava incompatibility issue
ENV HADOOP_HOME=/opt/hadoop/hadoop-3.3.0
ENV HIVE_HOME=/opt/hive/apache-hive-3.1.2-bin
RUN mv ${HIVE_HOME}/lib/guava-19.0.jar ${HIVE_HOME}/lib/guava-19.0.jar.save \
    && cp ${HADOOP_HOME}/share/hadoop/hdfs/lib/guava-27.0-jre.jar $HIVE_HOME/lib/

RUN apt-get -q update \
    && apt-get -q install -y --no-install-recommends \
        keychain  ssh  openssh-server sudo

# Install python and packages dependencies
RUN apt-get -q update \
    && apt-get -q install -y --no-install-recommends \
        python3-pip python3-venv python3-wheel

RUN pip3 install pandas sqlparse fastparquet pyarrow thrift

RUN apt-get -q update \
    && apt-get -q install -y --no-install-recommends \
        iperf3


COPY entry.sh /usr/local/bin/entry.sh
RUN chmod a+x /usr/local/bin/entry.sh

ENTRYPOINT ["/usr/local/bin/entry.sh"]

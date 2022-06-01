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

ENV HADOOP_VERSION=2.7.2
ENV HIVE_VERSION=2.3.8
ENV TEZ_VERSION=0.9.2
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV TEZ_HOME=/opt/tez/apache-tez-${TEZ_VERSION}-bin
ENV HIVE_HOME=/opt/hive/apache-hive-${HIVE_VERSION}-bin

RUN mkdir -p /opt/hadoop
ADD /hadoop-${HADOOP_VERSION}.tar.gz /opt/hadoop

RUN mkdir -p /opt/hive
ADD /apache-hive-${HIVE_VERSION}-bin.tar.gz /opt/hive

#RUN mkdir -p /opt/tez
#ADD /apache-tez-${TEZ_VERSION}-bin.tar.gz /opt/tez


RUN apt-get -q update \
    && apt-get -q install -y --no-install-recommends \
        keychain  ssh  openssh-server sudo

# Install python and packages dependencies
RUN apt-get -q update \
    && apt-get -q install -y --no-install-recommends \
        python3-pip python3-venv python3-wheel vim

RUN pip3 install pandas sqlparse fastparquet pyarrow thrift

# install build essential for compiling within docker
RUN apt-get -q update \
    && apt-get -q install -y build-essential maven autoconf automake libtool vim

# Install protobuf 2.5.0 for tez
ADD /protobuf-2.5.0.tar.gz /tmp
RUN cd /tmp/protobuf-2.5.0 \
    && ./configure --prefix=/usr --disable-shared \
    && make && make install

ADD /tez /tmp/tez
RUN cd /tmp/tez \
    && mvn package -Pdist -DskipTests -Dtar -Dmaven.javadoc.skip=true \
    && mkdir -p ${TEZ_HOME} \
    && tar -xvf tez-dist/target/tez-0.9.2-minimal.tar.gz -C ${TEZ_HOME}

# Fix multiple SLF4J binding conflict.
RUN rm ${TEZ_HOME}/lib/slf4j-log4j12-1.7.10.jar
RUN rm ${HIVE_HOME}/lib/log4j-slf4j-impl-2.6.2.jar

# Workaround the guava problem. Tez-0.9.2 uses guava 19 content but hive 2.3.8 has guava 14
RUN mv ${HIVE_HOME}/lib/guava-14.0.1.jar ${HIVE_HOME}/lib/guava-14.0.1.jar.save

COPY entry.sh /usr/local/bin/entry.sh
RUN chmod a+x /usr/local/bin/entry.sh

ENTRYPOINT ["/usr/local/bin/entry.sh"]

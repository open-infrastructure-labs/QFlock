#!/bin/bash

set -e
#set -x
echo "Building option: [$1]"

# Start fresh, so remove the spark home directory.
echo "Removing $SPARK_HOME"
rm -rf $SPARK_HOME

# Build Spark
cd $SPARK_SRC
if [ ! -d $SPARK_BUILD ]; then
  echo "Creating Build Directory"
  mkdir $SPARK_BUILD
fi
if [ ! -d $SPARK_BUILD/spark-events ]; then
  mkdir $SPARK_BUILD/spark-events
fi

# Only build spark if it was requested, since it takes so long.
if [ "$1" != "spark" ]; then
  pushd build
  # Download and Install Spark.
  if [ ! -f $SPARK_PACKAGE_DL ]; then
    echo "Downloading Spark: $SPARK_PACKAGE_URL"
    wget $SPARK_PACKAGE_URL
  else
    echo "$SPARK_PACKAGE_DL already exists, skip download."
  fi
  # Extract our built package into our install directory.
  echo "Extracting $SPARK_PACKAGE_DL to $SPARK_HOME"
  tar -xzf $SPARK_PACKAGE_DL -C /build \
    && mv $SPARK_BUILD/spark-3.2.0-bin-hadoop2.7 $SPARK_HOME
  popd
else
  echo "Building spark"
  rm $SPARK_SRC/spark-*SNAPSHOT*.tgz || true
  ./dev/make-distribution.sh --name custom-spark --pip --tgz -Pkubernetes
  # Install Spark.
  # Extract our built package into our install directory.
  echo "Extracting $SPARK_PACKAGE.tgz -> $SPARK_HOME"
  sudo tar -xzf "$SPARK_SRC/$SPARK_PACKAGE.tgz" -C $SPARK_BUILD \
  && mv $SPARK_BUILD/$SPARK_PACKAGE $SPARK_HOME
fi


#!/bin/bash

# docker kill sparkworker
# docker kill sparkmaster
# docker kill qflock-spark-dc1

QFLOCK_SPARK=$(docker container ls -q --filter name="qflock-spark*")

if [ ! -z "$QFLOCK_SPARK" ]
then
  docker container stop ${QFLOCK_SPARK}
fi

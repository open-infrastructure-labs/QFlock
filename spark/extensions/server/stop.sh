#!/bin/bash

QFLOCK_SPARK2=$(docker container ls -q --filter name="qflock-spark-dc2*")

if [ ! -z "$QFLOCK_SPARK2" ]
then
  docker container stop ${QFLOCK_SPARK2}
fi
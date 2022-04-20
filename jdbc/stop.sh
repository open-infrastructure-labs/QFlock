#!/bin/bash

QFLOCK_JDBC=$(docker container ls -q --filter name="qflock-jdbc*")

if [ ! -z "$QFLOCK_JDBC" ]
then
  docker container stop ${QFLOCK_JDBC}
fi
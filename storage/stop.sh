#!/usr/bin/env bash

QFLOCK_STORAGE=$(docker container ls -q --filter name="qflock-storage*")

if [ ! -z "$QFLOCK_STORAGE" ]
then
  docker container stop ${QFLOCK_STORAGE}
fi
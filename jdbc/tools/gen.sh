#!/bin/bash

pushd "$(dirname "$0")" # Set root to script dir.

thrift -gen java -strict -out ../driver/src/main/java jdbc.thrift
thrift -gen py -strict -out ../server/ jdbc.thrift

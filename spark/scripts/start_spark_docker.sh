#!/bin/bash

# Script to start up the spark docker.

pushd "$(dirname "$0")"

# first start ssh
echo "Starting SSH Server"
sudo service ssh start
echo "Starting SSH Server.  Done."

# next pause the docker so it does not exit.
sleep infinity
#! /bin/bash

#set -n
ps -ef | grep "/bin/bash /extensions/server/server.sh" | tr -s ' '  | cut -d' ' -f2 | xargs kill -9 | true
ps -ef | grep "/bin/bash /extensions/server/scripts/start-remote-daemon.sh restart" | tr -s ' '  | cut -d' ' -f2 | xargs kill -9 | true
ps -ef | grep "/usr/lib/jvm/java-8-openjdk-amd64/bin/java -cp /conf:/opt/spark-3.3.0" | tr -s ' '  | cut -d' ' -f2 | xargs kill -9 | true
rm -rf /tmp/spark-temp
pushd /extensions/server
/extensions/server/scripts/start-remote-daemon.sh restart &

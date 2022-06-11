#! /bin/bash

#set -n
ps -ef | grep "python3 /jdbc/server/jdbc_server.py" | tr -s ' '  | cut -d' ' -f2 | xargs kill -9 | true
ps -ef | grep "/bin/bash /scripts/start-jdbc-daemon.sh restart" | tr -s ' '  | cut -d' ' -f2 | xargs kill -9 | true

pushd /jdbc/server
/scripts/start-jdbc-daemon.sh restart &

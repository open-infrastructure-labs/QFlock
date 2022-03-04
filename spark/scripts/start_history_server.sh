sudo cp history-server.properties conf/master
docker exec -it sparkmaster ./sbin/start-history-server.sh --properties-file /conf/history-server.properties

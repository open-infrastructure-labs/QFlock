
pushd $SPARK_HOME
DEBUG_STRING=--conf="spark.driver.extraJavaOptions=-classpath /conf/:/opt/spark-3.2.1/jars/*: -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=172.18.0.3:5006"

SPARK_WORKER_INSTANCES=1 SPARK_WORKER_CORES=1 ./sbin/start-thriftserver.sh --master local\
     --total-executor-cores 1\
     --conf "spark.cores.max=1"\
     --conf "spark.driver.maxResultSize=2g"\
     --conf "spark.driver.memory=20g"\
     --conf "spark.executor.memory=20g"\
     --conf "spark.sql.hive.metastore.version=3.1.2"\
     --conf "spark.sql.hive.metastore.jars=path"\
     --conf "spark.sql.hive.metastore.jars.path=/qflock/benchmark/src/jars/*.jar"\
     --conf "spark.sql.catalogImplementation=hive"\
     --conf "spark.sql.cbo.enabled=true"\
     --conf "spark.sql.cbo.planStats.enabled=true"\
     --conf "spark.sql.statistics.histogram.enabled=true"\
     --conf "spark.sql.warehouse.dir=hdfs://qflock-storage:9000/user/hive/warehouse3"\
     --hiveconf "hive.metastore.uris=thrift://qflock-storage:9083" \
     --hiveconf "metastore.catalog.default=spark_dc" \
     --hiveconf "hive.metastore.warehouse.dir=hdfs://qflock-storage:9000/user/hive/warehouse"\
     --hiveconf "hive.metastore.local=false"\
     --hiveconf "hive.server2.thrift.max.worker.threads=7"\
     --hiveconf "hive.server2.thrift.min.worker.threads=5"\
     --packages org.apache.spark:spark-hive_2.12:3.2.1 > /opt/volume/logs/hiveserver2.log 2>&1 &

     # --conf "spark.driver.extraJavaOptions=-classpath /conf/:/opt/spark-3.2.1/jars/*: -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=172.18.0.3:5006"\
 #--conf "spark.driver.extraJavaOptions=-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"\

echo "HIVESERVER2_READY"
echo "HIVESERVER2_READY" > /opt/volume/status/HIVESERVER2_STATE

echo "RUNNING_MODE $RUNNING_MODE"

if [ "$RUNNING_MODE" = "daemon" ]; then
    sleep infinity
fi

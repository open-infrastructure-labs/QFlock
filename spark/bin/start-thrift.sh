
pushd $SPARK_HOME
DEBUG_STRING=--conf="spark.driver.extraJavaOptions=-classpath /conf/:/opt/spark-3.2.1/jars/*: -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=172.18.0.3:5006"

./sbin/start-thriftserver.sh --master local\
     --conf "spark.driver.maxResultSize=2g"\
     --conf "spark.driver.memory=2g"\
     --conf "spark.executor.memory=2g"\
     --conf "spark.sql.hive.metastore.version=3.1.2"\
     --conf "spark.sql.hive.metastore.jars=path"\
     --conf "spark.sql.hive.metastore.jars.path=/qflock/benchmark/src/jars/*.jar"\
     --conf "spark.sql.catalogImplementation=hive"\
     --conf "spark.sql.warehouse.dir=hdfs://qflock-storage:9000/user/hive/warehouse3"\
     --conf "spark.sql.cbo.enabled=true"\
     --conf "spark.sql.cbo.planStats.enabled=true"\
     --conf "spark.sql.statistics.histogram.enabled=true"\
     --hiveconf "hive.metastore.uris=thrift://qflock-storage:9083" \
     --hiveconf "metastore.catalog.default=spark_dc" \
     --hiveconf "hive.metastore.warehouse.dir=hdfs://qflock-storage:9000/user/hive/warehouse"\
     --hiveconf "hive.metastore.local=false"\
     --packages org.apache.spark:spark-hive_2.12:3.2.1

     # --conf "spark.driver.extraJavaOptions=-classpath /conf/:/opt/spark-3.2.1/jars/*: -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=172.18.0.3:5006"\

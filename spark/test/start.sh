echo "$1"

spark-submit --master local[4] --total-executor-cores 1 \
--class com.github.qflock.QflockTest \
--conf "spark.driver.maxResultSize=16g"  \
--conf "spark.driver.memory=16g"  \
--conf "spark.executor.memory=16g"  \
--conf "spark.sql.execution.arrow.enabled=true"  \
--conf "spark.sql.catalogImplementation=hive"  \
--conf "spark.sql.warehouse.dir=hdfs://qflock-storage-dc1:9000/user/hive/warehouse3"  \
--conf "spark.hadoop.dfs.blocksize=16777216"  \
--conf "spark.hadoop.parquet.block.size=16777216"  \
--conf "spark.sql.cbo.enabled=true"  \
--conf "spark.sql.statistics.histogram.enabled=true"  \
--jars /extensions/target/scala-2.12/qflock-extensions_2.12-0.1.0.jar \
--conf spark.hadoop.hive.metastore.uris=thrift://qflock-storage-dc1:9084 \
/qflock/spark/test/target/scala-2.12/qflock-test_2.12-0.1.0.jar "$1" $2

#--conf 'spark.driver.extraJavaOptions=-classpath /conf/:/opt/spark-3.2.2/jars/*: -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=192.168.48.3:5006' \

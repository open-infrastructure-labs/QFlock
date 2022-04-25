/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.qflock.datasource

import java.util

import scala.collection.JavaConverters._

import com.github.qflock.datasource.common.Pushdown
import com.github.qflock.datasource.hdfs.{HdfsScan, HdfsStore}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.slf4j.LoggerFactory

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SessionConfigSupport, SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.hive.extension.ExtHiveUtils
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** Creates a data source object for Spark that
 *  supports pushdown of predicates such as Filter, Project .
 *
 */
class QflockDatasource extends TableProvider
  with SessionConfigSupport with DataSourceRegister {

  private val logger = LoggerFactory.getLogger(getClass)
  override def toString: String = s"QflockDs()"
  override def supportsExternalMetadata(): Boolean = true

  private val sparkSession: SparkSession = SparkSession
      .builder()
      .getOrCreate()
  private var path: String = ""
  private def getPath(dbName: String, tableName: String): String = {
    if (path != "") path
    else {
      val table = ExtHiveUtils.getTable(dbName, tableName)
      val sd = table.getSd()
      sd.getLocation()
    }
  }
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {

    val path = getPath(options.get("dbName"), options.get("tableName"))
    if (options.get("format") == "parquet") {
      /* With parquet, we infer the schema from the metadata.
       */
      // logger.info(s"inferSchema path: ${path}")
      val fileStatusArray = HdfsStore.getFileStatusList(path)
      val schema = ParquetUtils.inferSchema(sparkSession, options.asScala.toMap, fileStatusArray)
      schema.get
    } else {
      /* Other types like CSV require a user-supplied schema */
      throw new IllegalArgumentException("requires a user-supplied schema")
    }
  }

  override def getTable(schema: StructType,
                        transforms: Array[Transform],
                        options: util.Map[String, String]): Table = {
    val path = getPath(options.get("dbName"), options.get("tableName"))
    logger.trace("getTable: Options " + options)
    new QflockBatchTable(schema, options, path)
  }

  override def keyPrefix(): String = {
    "qflockDs"
  }
  override def shortName(): String = "qflockDs"
}

/** Creates a Table object that supports pushdown predicates
 *   such as Filter, Project.
 *
 * @param schema the StructType format of this table
 * @param options the parameters for creating the table
 *                "endpoint" is the server name,
 *                "accessKey" and "secretKey" are the credentials for above server.
 *                 "path" is the full path to the file.
 */
class QflockBatchTable(schema: StructType,
                       options: util.Map[String, String],
                       path: String)
  extends Table with SupportsRead {

  private val logger = LoggerFactory.getLogger(getClass)
  logger.trace("Created")
  override def name(): String = this.getClass.toString

  override def schema(): StructType = schema

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(params: CaseInsensitiveStringMap): ScanBuilder =
      new QflockScanBuilder(schema, options, path)
}

/** Creates a builder for scan objects.
 *  For hdfs HdfsScan.
 *
 * @param schema the format of the columns
 * @param options the options (see PushdownBatchTable for full list.)
 */
class QflockScanBuilder(schema: StructType,
                        options: util.Map[String, String],
                        path: String)
  extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns {

  private val logger = LoggerFactory.getLogger(getClass)
  var pushedFilter: Array[Filter] = new Array[Filter](0)
  private var prunedSchema: StructType = schema

  /** Returns a scan object for this particular query.
   *   Currently we only support Hdfs.
   *
   * @return the scan object a HdfsScan
   */
  override def build(): Scan = {
    /* Make the map modifiable.
     * The objects below can override defaults.
     */
    val opt: util.Map[String, String] = new util.HashMap[String, String](options)

    if (!path.contains("hdfs")) {
      throw new Exception(s"endpoint ${options.get("endpoint")} is unexpected")
    }
    opt.put("path", path)
    new HdfsScan(schema, opt, pushedFilter, prunedSchema)
  }
  /** returns true if pushdowns are supported for this type of connector.
   *
   * @return true if pushdown supported, false otherwise
   */
  private def pushdownSupported(): Boolean = {
    if (!path.contains("hdfs")) {
      throw new Exception(s"path ${path} is unexpected")
    }
    HdfsStore.pushdownSupported(options)
  }
  /** returns true if filters can be fully pushed down
   *
   * @return true if pushdown supported, false otherwise
   */
  private def filterPushdownFullySupported(): Boolean = {

    if (!path.contains("hdfs")) {
      throw new Exception(s"path ${path} is unexpected")
    }
    HdfsStore.filterPushdownFullySupported(options)
  }
  /** Pushes down the list of columns specified by requiredSchema
   *
   * @param requiredSchema the list of coumns we should use, and prune others.
   */
  override def pruneColumns(requiredSchema: StructType): Unit = {
    if (pushdownSupported() && !options.containsKey("DisableProjectPush")) {
      prunedSchema = requiredSchema
      logger.info("pruneColumns " + requiredSchema.toString)
    }
  }

  override def pushedFilters: Array[Filter] = {
    logger.trace("pushedFilters" + pushedFilter.toList)
    pushedFilter
  }

  /** Pushes down a list of filters.  We assume the filters
   *  are "and" separated. e.g. filter1 and filter2 and filter3, etc.
   *
   * @param filters the list of filters to push down
   * @return list of filters to be re-evaluated upon completion of query.
   */
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    logger.trace("pushFilters" + filters.toList)
    if (!pushdownSupported() || options.containsKey("DisableFilterPush")) {
      filters
    } else {
    val pushdown = new Pushdown(schema, prunedSchema, pushedFilter, None, options)
      val f = filters.map(f => pushdown.buildFilterExpression(f))
      logger.trace("compiled filter list: " + f.mkString(", "))
      if (!f.contains(None)) {
        pushedFilter = filters
        if (filterPushdownFullySupported()) {
          // return empty array to indicate we pushed down all the filters.
          Array[Filter]()
        } else {
          // In this case we know that we need to re-evaluate the filters
          // since the pushdown cannot guarantee application of the filter.
          filters
        }
      } else {
        logger.info("Not pushing down filters.")
        // If we return all filters it will indicate they need to be re-evaluated.
        filters
      }
    }
  }
}

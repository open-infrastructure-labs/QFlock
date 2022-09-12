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
package com.github.qflock.server

import com.github.qflock.datasource.QflockTableDescriptor
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Table}
import org.slf4j.LoggerFactory

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.extension.ExtHiveUtils

/** Is a table represented by a hive table instance, and a
 *  set of temporary spark view which this object creates.
 * @param dbName database name as it appears in metastore.
 * @param tableName name of the table as it appears in metastore.
 * @param maxViews number of views to create for this table.
 */
class QflockServerTable(dbName: String, tableName: String, maxViews: Integer = 4) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val spark = SparkSession.builder.getOrCreate
  private val table: Table = ExtHiveUtils.getTable(dbName, tableName)
  def getTableName: String = tableName
  private val schema = getSchema
  def getSchema: String = {
    def convert_col(dType: String): String = {
      dType match {
        case "bigint" => "long"
        case "double" => "double"
        case "string" => "string"
      }
    }
    // each field in the schema has name:type:nullable
    val s = table.getSd.getCols.toArray.map(col => {
      val c = col.asInstanceOf[FieldSchema]
      s"${c.getName}:${convert_col(c.getType)}:true"
    })
    s.mkString(",")
  }

  /** Creates a spark view for the given table with a specific
   *  request id.  This request id refers to the
   *  set of parameters we are passing it in the QflockTableDescriptor.
   * @param requestId request id of qflock table descriptor.
   */
  def createView(requestId: Integer): Unit = {
    val df = spark.read
      .format("qflockDs")
      .option("format", "parquet")
      .option("schema", schema)
      .option("tableName", table.getTableName)
      .option("path", table.getSd.getLocation)
      .option("dbName", table.getDbName)
      .option("requestId", requestId.toString)
      .load()
    val viewName = s"${table.getTableName}_$requestId"
    logger.info(s"Create view for table: ${table.getDbName}:${table.getTableName} " +
                s"request_id: $requestId " +
                s"viewName: $viewName")
    df.createOrReplaceTempView(viewName)
  }
  def createViews(): Unit = {
    for (requestId <- Range(0, maxViews)) {
      createView(requestId)
    }
  }
  // fillRequestInfo
  // freeRequestId
  val descriptor: QflockTableDescriptor = {
    // Tell the datasource about our table and the number of views it has.
    // This table descriptor will be used later to fetch a request id
    // via fillRequestInfo()
    // That request id can then be used to ship parameters to our
    // Datasource which is selecting ranges of row groups to read.
    QflockTableDescriptor.addTable(table.getTableName, maxViews)
    QflockTableDescriptor.getTableDescriptor(table.getTableName)
  }
}

object QflockServerTable {

  def getTables(dbName: String): Array[QflockServerTable] = {
    ExtHiveUtils.getAllTables(dbName).map { t =>
      new QflockServerTable(dbName, t)
    }
  }
  def getAllTables: Array[QflockServerTable] = {
    ExtHiveUtils.getDatabases().flatMap { d =>
      getTables(d)
    }
  }
}

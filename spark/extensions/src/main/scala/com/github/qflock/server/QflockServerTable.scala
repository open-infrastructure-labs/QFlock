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

import org.apache.hadoop.hive.metastore.api.Table
import org.slf4j.LoggerFactory

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.extension.ExtHiveUtils


class QflockServerTable(dbName: String, tableName: String, maxViews: Integer = 4) {
  private val logger = LoggerFactory.getLogger(getClass)
  val spark = SparkSession.builder.getOrCreate
  val table: Table = ExtHiveUtils.getTable(dbName, tableName)
  val schema = getSchema
  def getSchema: String = {
    def convert_col(dType: String): String = {
      dType match {
        case "bigint" => "long"
        case "double" => "double"
        case "string" => "string"
      }
    }
      // each field in the schema has name:type:nullable
//      table.getSd().getCols().toArray.map(col =>
//        s"${col.getName}:${convert_col(col.getType)}:true")
    ""
  }
  def createView(requestId: Integer): Unit = {
    logger.info(s"Create view for table: ${table.getTableName} request_id: ${requestId}")
  }
//    df = spark.read
//      .format("qflockDs")
//      .option("format", "parquet")
//      .option("schema", schema)
//      .option("tableName", table.tableName)
//      .option("path", filePath)
//      .option("dbName", table.dbName)
//      .option("requestId", requestId)
//      .load()
//    view_name = s"{$table.tableName}_{$requestId}"
//    df.createOrReplaceTempView(view_name)
//  }
  def createViews: Unit = {
    for (requestId <- Range(0, maxViews)) {
      createView(requestId)
    }
  }
}

object QflockServerTable {

  def getTables(dbName: String): Array[QflockServerTable] = {
    ExtHiveUtils.getAllTables(dbName).map { t =>
      new QflockServerTable(dbName, t)
    }
  }
  def getAllTables: Array[QflockServerTable] = {
    ExtHiveUtils.getDatabases.map { d =>
      getTables(d)
    }.flatten
  }
}

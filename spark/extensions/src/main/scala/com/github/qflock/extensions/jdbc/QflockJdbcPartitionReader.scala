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
package com.github.qflock.extensions.jdbc

import java.sql.{Connection, DriverManager, ResultSet}
import java.util
import java.util.Properties

import org.slf4j.LoggerFactory

import org.apache.spark.sql.QflockJdbcUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader

/** PartitionReader of JDBC
 *  This iterator returns an internal row.
 *
 * @param options the options including "path"
 * @param partition the QflockJdbcPartition to read from
 */
class QflockJdbcPartitionReader(options: util.Map[String, String],
                                partition: QflockJdbcPartition)
  extends PartitionReader[InternalRow] {

  private val logger = LoggerFactory.getLogger(getClass)
  private val (rowIterator: Iterator[InternalRow], connection: Option[Connection]) = {
    val query = options.get("query")
    val driver = options.get("driver")
    val url = options.get("url")
    try {
      logger.debug("Loading " + driver)
      // scalastyle:off classforname
      Class.forName(driver).newInstance
      // scalastyle:on classforname
    } catch {
      case e: Exception =>
        logger.warn("Failed to load JDBC driver.")
        logger.warn(e.toString)
    }
    val (resultSet: ResultSet, con: Option[Connection]) = {
      logger.debug(s"connecting to $url")
      val properties = new Properties
      properties.setProperty("compression", "true")
      properties.setProperty("bufferSize", "42")
      properties.setProperty("rowGroupOffset", partition.offset.toString)
      properties.setProperty("rowGroupCount", partition.length.toString)
      properties.setProperty("tableName", options.get("tableName"))
      properties.setProperty("queryStats", options.get("queryStats"))
      properties.setProperty("resultApi", "default")
      properties.setProperty("queryName", options.get("queryname"))
      properties.setProperty("appId", options.get("appid"))
      val con = DriverManager.getConnection(url, properties)
      logger.debug(s"connected to $url")
      val select = con.prepareStatement(query)
      logger.debug(s"Starting query $query")
      val result = select.executeQuery(query)
      logger.debug(s"Query complete $query")
      // return the result and the connection so we can close it later.
      (result, Some(con))
    }
    (QflockJdbcUtil.getResultSetRowIterator(resultSet), con)
  }

  var index = 0
  def next: Boolean = {
    val n = rowIterator.hasNext
    if (!n) {
      logger.info(s"get: partition: ${partition.index} ${partition.offset}" +
        s" ${partition.length} ${partition.name} index: $index")
    }
    n
  }
  def get: InternalRow = {
    val row = rowIterator.next
    if ((index % 500000) == 0) {
      logger.info(s"get: partition: ${partition.index} ${partition.offset}" +
                  s" ${partition.length} ${partition.name} index: $index")
    }
    index = index + 1
    row
  }

  def close(): Unit = {
    if (connection.isDefined) {
      connection.get.close()
    }
  }
}

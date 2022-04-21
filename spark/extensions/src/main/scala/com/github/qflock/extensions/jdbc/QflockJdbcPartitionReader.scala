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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{QflockJdbcUtil, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader


/** PartitionReader of JDBC
 *
 * @param options the options including "path"
 * @param partition the QflockJdbcPartition to read from
 */
class QflockJdbcPartitionReader(options: util.Map[String, String],
                                partition: QflockJdbcPartition,
                                sparkSession: SparkSession,
      sharedConf: Broadcast[org.apache.spark.util.SerializableConfiguration])
  extends PartitionReader[InternalRow] {

  private val logger = LoggerFactory.getLogger(getClass)
  private var (rowIterator: Iterator[InternalRow], connection: Option[Connection]) = {
    var query = options.get("query")
    var driver = options.get("driver")
    var url = options.get("url")
    try {
      logger.info("Loading " + driver)
      // scalastyle:off classforname
      Class.forName(driver).newInstance
      // scalastyle:on classforname
    } catch {
      case e: Exception =>
        logger.warn("Failed to load JDBC driver.")
    }
    val (resultSet: ResultSet, con: Option[Connection]) = {
      logger.info(s"connecting to ${url}")
      val properties = new Properties
      properties.setProperty("compression", "true")
      properties.setProperty("bufferSize", "42")
      val con = DriverManager.getConnection(url, properties)
      logger.info(s"connected to ${url}")
      val select = con.prepareStatement(query)
      logger.info(s"Starting query ${query}")
      val result = select.executeQuery(query)
      logger.info(s"Query complete ${query}")
      // return the result and the connection so we can close it later.
      (result, Some(con))
    }
    (QflockJdbcUtil.getResultSetRowIterator(resultSet), con)
  }

  var index = 0
  def next: Boolean = {
    rowIterator.hasNext
  }
  def get: InternalRow = {
    val row = rowIterator.next
    if (((index % 500000) == 0) ||
        (!next)) {
      logger.info(s"get: partition: ${partition.index} ${partition.offset}" +
                  s" ${partition.length} ${partition.name} index: ${index}")
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

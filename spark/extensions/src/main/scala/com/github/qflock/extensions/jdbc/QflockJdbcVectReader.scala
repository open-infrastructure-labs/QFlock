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

import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.vectorized.ColumnVector


/** Allows for reading batches of columns from NDP
 *  using the NDP binary columnar format.
 *
 *  @param schema description of columns
 *  @param batchSize the number of rows in a batch
 *  @param part the current hdfs partition
 *  @param options data source options.
 */
class QflockJdbcVectReader(schema: StructType,
                           batchSize: Integer,
                           part: QflockJdbcPartition,
                           options: util.Map[String, String]) extends QflockColVectReader {
  private val logger = LoggerFactory.getLogger(getClass)
  def next(): Boolean = {
    nextBatch()
  }
  def get(): ColumnarBatch = {
    columnarBatch
  }
  private var connection: Option[Connection] = None
  def close(): Unit = {
    if (connection.isDefined) {
      connection.get.close()
      connection = None
    }
  }
  private var rowsReturned: Long = 0
  private var currentBatchSize: Int = 0
  private var batchIdx: Long = 0
  private val numCols = schema.fields.length
  private val colVectors = QflockJdbcColumnVector.apply(batchSize, schema)
  private val columnarBatch = new ColumnarBatch(colVectors.asInstanceOf[Array[ColumnVector]])
  /** Fetches the next set of columns from the stream, returning the
   *  number of rows that were returned.
   *  We expect all columns to return the same number of rows.
   *
   *  @return Integer, the number of rows returned for the batch.
   */
  private var results: Option[ResultSet] = None
  private def readNextBatch(): Integer = {
    if (rowsReturned > 0) {
      return 0
    }
    results = Some(getResults)
    var rows: Integer = 0
    for (i <- 0 until numCols) {
      val currentRows = colVectors(i).setupColumn(i, part, results.get)
      if (rows == 0) {
        // logger.info(s"readNextBatch found rows: $currentRows")
        rows = currentRows
      } else if (rows != 0 && currentRows != rows) {
        // We expect all rows in the batch to be the same size.
        throw new Exception(s"mismatch in rows $currentRows != $rows")
      }
    }
    rows
  }
  def getResults: ResultSet = {
    val query = options.get("query")
    val driver = options.get("driver")
    val url = options.get("url")
    try {
      logger.info("Loading " + driver)
      // scalastyle:off classforname
      Class.forName(driver).newInstance
      // scalastyle:on classforname
    } catch {
      case e: Exception =>
        logger.warn("Failed to load JDBC driver." + e.toString)
    }
    logger.debug(s"connecting to $url")
    val properties = new Properties
    properties.setProperty("compression", "true")
    properties.setProperty("bufferSize", "42")
    properties.setProperty("rowGroupOffset", part.offset.toString)
    properties.setProperty("rowGroupCount", part.length.toString)
    properties.setProperty("tableName", options.get("tableName"))
    properties.setProperty("queryStats", options.get("queryStats"))
    connection = Some(DriverManager.getConnection(url, properties))
    logger.debug(s"connected to $url")
    val select = connection.get.prepareStatement(query)
    logger.debug(s"Starting query $query")
    val result = select.executeQuery(query)
    logger.debug(s"Query complete $query")
    // return the result and the connection so we can close it later.
    result
  }
  /**
   * Advances to the next batch of rows. Returns false if there are no more.
   * @return Boolean, true if more rows, false if none.
   */
  private def nextBatch(): Boolean = {
    columnarBatch.setNumRows(0)
    val rows = readNextBatch()
    if (rows == 0) {
      // logger.info(s"nextBatch Done rows: $rows total: $rowsReturned")
    }
    rowsReturned += rows
    columnarBatch.setNumRows(rows.toInt)
    currentBatchSize = rows
    batchIdx = 0
    if (rows > 0) {
      true
    } else {
      false
    }
  }
}
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

import com.github.qflock.extensions.common.QflockQueryCache
import com.github.qflock.jdbc.QflockResultSet
import org.slf4j.LoggerFactory

import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.vectorized.ColumnVector


/** Allows for reading batches of columns from NDP
 *  using the NDP binary columnar format.
 *
 *  @param schema description of columns
 *  @param part the current jdbc partition
 *  @param options data source options.
 */
class QflockJdbcVectorReader(schema: StructType,
                             part: QflockJdbcPartition,
                             options: util.Map[String, String]) extends QflockColumnarVectorReader {
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
  private val colVectors = QflockJdbcColumnVector.apply(schema)
  private val columnarBatch = new ColumnarBatch(colVectors.asInstanceOf[Array[ColumnVector]])
  private var results: Option[ResultSet] = None
  /** Fetches the next set of columns from the stream, returning the
   *  number of rows that were returned.
   *  We expect all columns to return the same number of rows.
   *
   *  @return Integer, the number of rows returned for the batch.
   */
  private def readNextBatch(): Integer = {
    if (rowsReturned > 0) {
      // There is only one batch.  So if we have already returned that
      // batch, then we are done, just return 0 to indicate done.
      return 0
    }
    results = Some(getResults)
    var rows: Integer = 0
    for (i <- 0 until numCols) {
      val currentRows = colVectors(i).setupColumn(i, results.get)
      if (rows == 0) {
        logger.trace(s"readNextBatch found rows: $currentRows")
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
    val appId = options.get("appid")
    val queryName = options.getOrDefault("queryname", "")
    val cachedValue = QflockQueryCache.checkKey(query, part.index)
    if (cachedValue.isDefined) {
      val bytes = QflockQueryCache.bytes
      QflockLog.log(s"queryName:$queryName use-cached-data " +
                    s"appId:$appId part:${part.index} cachedBytes:$bytes key:$query")
      cachedValue.get.asInstanceOf[ResultSet]
    } else {
      val res = getRemoteResults
      val qfResultSet = res.asInstanceOf[QflockResultSet]
      val cached = QflockQueryCache.insertData(query, part.index, res, qfResultSet.getSize)
      if (cached) {
        val bytes = QflockQueryCache.bytes
        QflockLog.log(s"queryName:$queryName cache-data " +
          s"appId: $appId part: ${part.index} cachedBytes:$bytes key: $query")
      }
      res
    }
  }

  def getRemoteResults: ResultSet = {
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
        logger.warn("Failed to load JDBC driver." + e.toString)
    }

    logger.debug(s"connecting to $url")
    val properties = new Properties
    properties.setProperty("compression", "true")
    properties.setProperty("rowGroupOffset", part.offset.toString)
    properties.setProperty("rowGroupCount", part.length.toString)
    properties.setProperty("resultApi", "default")
    properties.setProperty("queryStats", options.getOrDefault("queryStats", ""))
    properties.setProperty("tableName", options.get("tablename"))
    properties.setProperty("queryName", options.get("queryname"))
    properties.setProperty("appId", options.get("appid"))
    val startTime = System.nanoTime()
    connection = Some(DriverManager.getConnection(url, properties))
    logger.debug(s"connected to $url")
    val select = connection.get.prepareStatement(query)
    logger.info(s"Starting query $query")
    val result = select.executeQuery(query)
    logger.info(s"Query complete $query")
    val elapsed = System.nanoTime() - startTime
    val qfResultSet = result.asInstanceOf[QflockResultSet]
    val appId = options.get("appid")
    val queryName = options.getOrDefault("queryname", "")
    val tableName = options.get("tablename")
    val ruleLog = options.get("rulelog")
    QflockLog.log(s"queryName:$queryName appId:$appId rows:${qfResultSet.getNumRows} " +
                  s"bytes:${qfResultSet.getSize} ruleLog:$ruleLog " +
                  s"tableName:$tableName part:${part.index} " +
                  s"timeNs:$elapsed query:$query",
                  path = options.get("resultspath"))
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

object QflockJdbcVectorReader {

  private val cache = collection.mutable.Map[String, ResultSet]()

  def checkCache(key: String): Option[ResultSet] = {
    cache.get(key)
  }
  def insertCache(cacheKey: String, value: ResultSet): Unit = {
    cache(cacheKey) = value
  }
  def removeCache(cacheKey: String, value: ResultSet): Unit = {
    cache(cacheKey) = value
  }
}

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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.extension.parquet.VectorizedParquetRecordReader
import org.apache.spark.sql.internal.SQLConf
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
class QflockJdbcParquetVectorReader(schema: StructType,
                                    part: QflockJdbcPartition,
                                    options: util.Map[String, String],
  sharedConf: Broadcast[org.apache.spark.util.SerializableConfiguration],
  sqlConf: SQLConf)
  extends QflockColumnarVectorReader {
  private val logger = LoggerFactory.getLogger(getClass)
  def next(): Boolean = {
    nextBatch()
  }
  def get(): ColumnarBatch = {
    val batch = reader.get.getCurrentValue.asInstanceOf[ColumnarBatch]
    batch
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

  // Name the temp dir based on the appid, combined with the
  // partition index, which makes it unique.
  private val resultPath = s"/spark_rd/${options.get("appid")}_${part.index}"
  private var resultIndex = 0

  def getCurrentFile(): String = {
    val qfResultSet = results.get.asInstanceOf[QflockResultSet]
    s"${qfResultSet.tempDir}/part-$resultIndex.parquet"
  }
  private var reader: Option[VectorizedParquetRecordReader] = None
  /** Fetches the next set of columns from the stream, returning the
   *  number of rows that were returned.
   *  We expect all columns to return the same number of rows.
   *
   *  @return Integer, the number of rows returned for the batch.
   */
  private def readNextBatch(): Boolean = {
    // If the reader is already defined, just get the next batch from it.
    if (reader.isDefined && reader.get.nextKeyValue()) {
      true
    } else {
      // Either the reader is not defined yet, or it is defined,
      // but it is empty so we need the next one.
      val readerFactory = new HdfsColumnarPartitionReaderFactory(
        options, sharedConf, sqlConf)
      val qfResultSet = results.get.asInstanceOf[QflockResultSet]
      val partitions = qfResultSet.getResultFileCount()
      if (resultIndex < partitions) {
        val currentFile = getCurrentFile()
        reader = Some(readerFactory.createReader(currentFile))
        resultIndex += 1
        reader.get.nextKeyValue()
      } else {
        // Already ran through all partitions.
        false
      }
    }
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
    properties.setProperty("tempDir", resultPath)
    properties.setProperty("compression", "true")
    properties.setProperty("rowGroupOffset", part.offset.toString)
    properties.setProperty("rowGroupCount", part.length.toString)
    properties.setProperty("tableName", options.get("tablename"))
    properties.setProperty("queryStats", options.getOrDefault("queryStats", ""))
    properties.setProperty("resultApi", "parquet")
    properties.setProperty("queryName", options.get("queryname"))
    properties.setProperty("appId", options.get("appid"))
    val startTime = System.nanoTime()
    connection = Some(DriverManager.getConnection(url, properties))
    logger.debug(s"connected to $url")
    val select = connection.get.prepareStatement(query)
    logger.debug(s"Starting query $query")
    val result = select.executeQuery(query)
    logger.debug(s"Query complete $query")
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
    if (results == None) {
      // Read the results from jdbc server.
      // Once we have results we will iterate across the files.
      results = Some(getResults)
    }
    readNextBatch()
  }
}



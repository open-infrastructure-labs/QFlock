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
package com.github.qflock.extensions.remote

import com.github.qflock.extensions.common.QflockFileCachedData
import com.github.qflock.extensions.jdbc.QflockColumnarVectorReader
import com.github.qflock.server.QflockServerHeader
import org.slf4j.LoggerFactory

import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.vectorized.ColumnVector



/** Allows for reading batches of columns from NDP
 *  using the NDP compressed columnar format.
 *
 *  @param schema definition of the columns
 *  @param batchSize the number of rows in a batch
 *  @param query the text string of sql query
 *  @param client The client for reading the data
 *  @param cachedData The FileCachedData client for writing a cache file.
 */
class QflockRemoteColVectReader(schema: StructType,
                                batchSize: Integer,
                                query: String,
                                client: QflockClient,
                                cachedData: Option[QflockFileCachedData] = None)
    extends QflockColumnarVectorReader {
  private val logger = LoggerFactory.getLogger(getClass)
  override def next(): Boolean = {
    nextBatch()
  }
  override def get(): ColumnarBatch = {
    columnarBatch
  }
  override def close(): Unit = {
//    logger.info(s"Data Read Close $query")
    client.close()
    if (cachedData.isDefined) {
      cachedData.get.close()
    }
  }
  private val stream = client.getStream
  private var rowsReturned: Long = 0
  private var currentBatchSize: Int = 0
  private var batchIdx: Long = 0
  private def waitForBytes(context: String): Unit = {
    var waitCount: Long = 0
    while (stream.available() <= 0) {
      if (waitCount == 0) {
        logger.info(s"bytes not available $context, waiting ${client.toString}")
      }
      waitCount += 1
      Thread.sleep(1000)
    }
    if (waitCount > 0) {
      logger.info(s"bytes not available $context, waited $waitCount times ${client.toString}")
    }
  }
  private val (numCols: Integer, dataTypes: Array[Int]) = {
    /* The NDP server encodes the number of columns followed by
     * the the type of each column.  All values are doubles.
     */
    try {
      // waitForBytes("data type read")
//      logger.info(s"Data Read Starting $query")
      val magic = stream.readInt()
      if (magic != QflockServerHeader.magic) {
        throw new java.lang.IllegalStateException(s"magic $magic != ${QflockServerHeader.magic}")
      }
      val nColsLong = stream.readInt()
      val nCols: Integer = nColsLong
      logger.debug("nCols : " + String.valueOf(nCols) + " " + query)
      val dataTypes = new Array[Int](nCols)
      for (i <- 0 until nCols) {
        dataTypes(i) = stream.readInt()
         logger.debug(String.valueOf(i) + " : " + String.valueOf(dataTypes(i))
         + " " + query)
      }
      (nCols, dataTypes)
    } catch {
        case ex: Exception =>
        /* We do not expect to hit end of file, but if we do, it might mean that
         * the NDP query had nothing to return.
         */
          throw new Exception("Init Exception: " + ex)
          (0, new Array[Int](0))
        case ex: Throwable =>
          throw new Exception("Init Throwable: " + ex)
          (0, new Array[Int](0))
    }
  }
  def writeHeader(): Unit = {
    if (cachedData.isDefined && cachedData.get.shouldWrite) {
      val writeStream = cachedData.get.stream.get
      writeStream.writeInt(QflockServerHeader.magic)
      writeStream.writeInt(numCols)
      for (i <- 0 until numCols) {
        writeStream.writeInt(dataTypes(i))
      }
    }
  }
  writeHeader()
  private val colVectors = QflockRemoteColumnVector(batchSize, dataTypes, schema,
                                                     cachedData)
  private val columnarBatch = new ColumnarBatch(colVectors.asInstanceOf[Array[ColumnVector]])
  /** Fetches the next set of columns from the stream, returning the
   *  number of rows that were returned.
   *  We expect all columns to return the same number of rows.
   *
   *  @return Integer, the number of rows returned for the batch.
   */
  private def readNextBatch(): Integer = {
    var rows: Integer = 0
    for (i <- 0 until numCols) {
//      if (i != 0) {
//        waitForBytes(s"read col $i")
//      }
      val currentRows = colVectors(i).readColumn(stream)
//      logger.info(s"Data Read col $i rows $currentRows totalRows $rowsReturned $query")
      if (currentRows == 0) {
        // End of stream hit.
        return 0
      } else if (rows == 0) {
        rows = currentRows
      } else if (rows != 0 && currentRows != rows) {
        // We expect all rows in the batch to be the same size.
        logger.error(s"column $i $currentRows != $rows")
        throw new Exception(s"mismatch in rows $currentRows != $rows")
      }
    }
    rows
  }
  /**
   * Advances to the next batch of rows. Returns false if there are no more.
   * @return Boolean, true if more rows, false if none.
   */
  private def nextBatch(): Boolean = {
    columnarBatch.setNumRows(0)
    val rows = readNextBatch()
    if (rows == 0) {
//      logger.info(s"Data Read Batch Complete $query")
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

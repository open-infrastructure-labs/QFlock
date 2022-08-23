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
package com.github.qflock.extensions.compact

import java.io.DataInputStream

import com.github.qflock.extensions.jdbc.QflockColumnarVectorReader
import org.slf4j.LoggerFactory

import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.vectorized.ColumnVector


/** Allows for reading batches of columns from NDP
 *  using the NDP compressed columnar format.
 *
 *  @param schema definition of the columns
 *  @param batchSize the number of rows in a batch
 *  @param stream the data stream attached to server.
 */
class QflockCompactColVectReader(schema: StructType,
                                 batchSize: Integer,
                                 stream: DataInputStream)
    extends QflockColumnarVectorReader {
  private val logger = LoggerFactory.getLogger(getClass)
  override def next(): Boolean = {
    nextBatch()
  }
  override def get(): ColumnarBatch = {
    columnarBatch
  }
  override def close(): Unit = {
  }
  private var rowsReturned: Long = 0
  private var currentBatchSize: Int = 0
  private var batchIdx: Long = 0

  private val (numCols: Integer, dataTypes: Array[Int]) = {
    /* The NDP server encodes the number of columns followed by
     * the the type of each column.  All values are doubles.
     */
    try {
      // logger.info(s"Data Read Starting ${part.name}")
      // logger.info("reading cols from stream")
      val nColsLong = stream.readInt()
      val nCols: Integer = nColsLong
      // logger.info("nCols : " + String.valueOf(nCols))
      val dataTypes = new Array[Int](nCols)
      for (i <- 0 until nCols) {
        dataTypes(i) = stream.readInt()
        // logger.info(String.valueOf(i) + " : " + String.valueOf(dataTypes(i)))
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
  private val colVectors = QflockCompactColumnVector(batchSize, dataTypes, schema)
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
      val currentRows = colVectors(i).readColumn(stream)
      if (rows == 0) {
        rows = currentRows
      } else if (rows != 0 && currentRows != rows) {
        // We expect all rows in the batch to be the same size.
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
      // logger.info(s"Data Read Complete ${part.name}")
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

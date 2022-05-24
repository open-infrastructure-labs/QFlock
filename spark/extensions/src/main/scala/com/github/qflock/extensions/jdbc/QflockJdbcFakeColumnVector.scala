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

import java.nio.ByteBuffer
import java.sql.ResultSet

import org.slf4j.LoggerFactory

import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.unsafe.types.UTF8String

/** Represents a ColumnVector which can understand
 *  the NDP columnar binary format.
 *  @param batchSize the number of items in each row of a batch.
 *  @param dataType the Int representing the NdpDataType.
 *  @param schema the schema returned from the server.
 *  @return
 */
class QflockJdbcFakeColumnVector(batchSize: Integer, dataType: DataType, schema: StructType)
  extends ColumnVector(schema: StructType) {
  private val logger = LoggerFactory.getLogger(getClass)

  val typeSize: Int = {
    dataType match {
      case StringType => 8
      case IntegerType => 4
      case DoubleType => 8
      case LongType => 8
    }
  }
  val numBufferRows: Int = 32 * 1024
  val byteBuffer: ByteBuffer = {
    val bb = ByteBuffer.allocate(typeSize * numBufferRows)
    for (i <- 0 until numBufferRows) {
      dataType match {
        case StringType => bb.put("fake string".toByte)
        case IntegerType => bb.putInt(i * 4, 42)
        case DoubleType => bb.putDouble(i * 8, 42.34)
        case LongType => bb.putLong(i * 8, 42000)
      }
    }
    bb
  }
  def close(): Unit = {}
  def getArray(row: Int): org.apache.spark.sql.vectorized.ColumnarArray = { null }
  def getBinary(row: Int): Array[Byte] = { null }
  def getBoolean(row: Int): Boolean = { false }
  def getByte(row: Int): Byte = { byteBuffer.get(row) }
  def getChild(row: Int): org.apache.spark.sql.vectorized.ColumnVector = { null }
  def getDecimal(row: Int, r: Int, p: Int): org.apache.spark.sql.types.Decimal = { Decimal(0) }
  def getDouble(row: Int): Double = {
    val bufferRow = row % numBufferRows
    byteBuffer.getDouble(bufferRow * 8)
  }
  def getFloat(row: Int): Float = {
    val bufferRow = row % numBufferRows
    byteBuffer.getFloat(numBufferRows * 8)
  }
  def getInt(row: Int): Int = {
    val bufferRow = row % numBufferRows
    byteBuffer.getInt(bufferRow * 4)
  }
  def getLong(row: Int): Long = {
    val bufferRow = row % numBufferRows
    byteBuffer.getLong(bufferRow * 8)
  }
  def getMap(row: Int): org.apache.spark.sql.vectorized.ColumnarMap = { null }
  def getShort(row: Int): Short = { byteBuffer.getShort(row * 2) }
  def getUTF8String(row: Int): org.apache.spark.unsafe.types.UTF8String = {
    val bufferRow = row % numBufferRows
    UTF8String.fromBytes(byteBuffer.array(), bufferRow * 8, 8)
//    UTF8String.fromString("fakeString")
//    NdpDataType(dataType) match {
//      case NdpDataType.LongType => UTF8String.fromString(
//        byteBuffer.getLong(row * 8).toString)
//      case NdpDataType.DoubleType => UTF8String.fromString(
//        byteBuffer.getDouble(row * 8).toString)
//      case NdpDataType.ByteArrayType =>
//        if (fixedTextLen > 0) {
//          UTF8String.fromBytes(byteBuffer.array(), fixedTextLen * row, fixedTextLen)
//        } else {
//          val offset = stringIndex(row)
//          val length = stringLen.get(row)
//          UTF8String.fromBytes(byteBuffer.array(), offset, length)
//        }
//    }
  }
  def hasNull: Boolean = { false }
  def isNullAt(row: Int): Boolean = { false }
  def numNulls(): Int = { 0 }

  /** fetches the data for a columnar batch and
   *  returns the number of rows read.
   *  The data is read into the already pre-allocated
   *  arrays for the data.  Note that if the already allocated
   *  buffers are not big enough for the data, we will throw an Exception.
   *
   *  @return Int the number of rows returned.
   */
  def readColumn(colIdx: Integer, part: QflockJdbcPartition,
                 resultSet: ResultSet): Int = {
    part.rows.toInt
  }
}



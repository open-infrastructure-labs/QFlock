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

import java.io.{DataInputStream, EOFException, PrintWriter, StringWriter}
import java.nio.ByteBuffer

// ZSTD support
import com.github.luben.zstd.Zstd
import com.github.qflock.extensions.common.QflockFileCachedData
import com.github.qflock.server.QflockServerHeader
import org.slf4j.LoggerFactory

import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.unsafe.types.UTF8String

/** Represents a ColumnVector which can understand
 *  the compact columnar binary format.
 *  @param batchSize the number of items in each row of a batch.
 *  @param dataType the Int representing the QflockServerHeader.DataType.
 *  @param schema the schema returned from the server.
 *  @return
 */
class QflockRemoteColumnVector(batchSize: Integer,
                               dataType: Int,
                               schema: StructType,
                               id: String,
                               cachedData: Option[QflockFileCachedData] = None)
    extends ColumnVector(schema: StructType) {
  private val logger = LoggerFactory.getLogger(getClass)
  // private val factory = LZ4Factory.fastestInstance()
  // private val decompressor = factory.fastDecompressor()
  // val decompressor = factory.safeDecompressor()
  val (byteBuffer: ByteBuffer,
       bufferLength: Integer,
       compressedBuffer: ByteBuffer,
       compressedBufLen: Integer,
       stringIndex: Array[Int],
       stringLen: ByteBuffer) = {
    var stringIndex = Array[Int](0)
    var stringLen = ByteBuffer.allocate(0)
    val bytes: Int = QflockServerHeader.DataType(dataType) match {
      case QflockServerHeader.DataType.LongType => 8
      case QflockServerHeader.DataType.DoubleType => 8
      case QflockServerHeader.DataType.ByteArrayType =>
      // Assumes single byte length field.
      stringIndex = new Array[Int](batchSize * 4)
      stringLen = ByteBuffer.allocate(batchSize * 4)
      128
      case _ => 0
    }
    (ByteBuffer.allocate(batchSize * bytes),
     (batchSize * bytes).asInstanceOf[Integer],
     ByteBuffer.allocate(batchSize * bytes),
     (batchSize * bytes).asInstanceOf[Integer],
     stringIndex, stringLen)
  }
  private def readFully(stream: DataInputStream,
                        dest: Array[Byte],
                        offset: Int = 0,
                        length: Int): Unit = {
    stream.readFully(dest, offset, length)
    if (cachedData.isDefined && cachedData.get.shouldWrite) {
      cachedData.get.stream.get.write(dest, offset, length)
    }
  }
  private val header = ByteBuffer.allocate(4 * 4)
  private var fixedTextLen: Int = 0
  def close(): Unit = {}
  def getArray(row: Int): org.apache.spark.sql.vectorized.ColumnarArray = { null }
  def getBinary(row: Int): Array[Byte] = { null }
  def getBoolean(row: Int): Boolean = { false }
  def getByte(row: Int): Byte = { byteBuffer.get(row) }
  def getChild(row: Int): org.apache.spark.sql.vectorized.ColumnVector = { null }
  def getDecimal(row: Int, r: Int, p: Int): org.apache.spark.sql.types.Decimal = { Decimal(0) }
  def getDouble(row: Int): Double = { byteBuffer.getDouble(row * 8) }
  def getFloat(row: Int): Float = { byteBuffer.getFloat(row * 8) }
  def getInt(row: Int): Int = { byteBuffer.getInt(row * 4) }
  def getLong(row: Int): Long = { byteBuffer.getLong(row * 8) }
  def getMap(row: Int): org.apache.spark.sql.vectorized.ColumnarMap = { null }
  def getShort(row: Int): Short = { byteBuffer.getShort(row * 2) }
  def getUTF8String(row: Int): org.apache.spark.unsafe.types.UTF8String = {
    QflockServerHeader.DataType(dataType) match {
       case QflockServerHeader.DataType.LongType => UTF8String.fromString(
                                          byteBuffer.getLong(row * 8).toString)
      case QflockServerHeader.DataType.DoubleType => UTF8String.fromString(
                                          byteBuffer.getDouble(row * 8).toString)
      case QflockServerHeader.DataType.ByteArrayType =>
        if (fixedTextLen > 0) {
          UTF8String.fromBytes(byteBuffer.array(), fixedTextLen * row, fixedTextLen)
        } else {
          val offset = stringIndex(row)
          val length = stringLen.getInt(row * 4)
          UTF8String.fromBytes(byteBuffer.array(), offset, length)
        }
    }
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
   *  @param stream the stream of data with compact binary columnar format.
   *  @return Int the number of rows returned.
   */
  def readColumn(stream: DataInputStream): Int = {
    var rows: Int = 0
    try {
      readFully(stream, header.array(), 0, header.capacity())
      val headerBuf = header.array()
      if (headerBuf(0) == 0 && headerBuf(1) == 0 &&
          headerBuf(2) == 0 && headerBuf(3) == 0) {
        // logger.info("found terminator")
        return 0
      }
      val numBytes = header.getInt(QflockServerHeader.Offset.compressedLen)
      var compressed = numBytes > 0
      val tId = Thread.currentThread().getId
      QflockServerHeader.DataType(header.getInt(QflockServerHeader.Offset.dataType)) match {
        case QflockServerHeader.DataType.LongType =>
          logger.trace(s"$tId:$id) read $numBytes bytes (Long)")
          val dataBytes = header.getInt(QflockServerHeader.Offset.dataLen)
          if (compressed) {
              val cb = new Array[Byte](numBytes)
              readFully(stream, cb, 0, numBytes)
              logger.trace(s"$tId:$id) decompressing (Double)")
              Zstd.decompress(byteBuffer.array(), cb)
          } else {
            readFully(stream, byteBuffer.array(), 0, dataBytes)
          }
          rows = dataBytes / 8
          fixedTextLen = 0
          logger.trace(s"$tId:$tId:$id) decompressed $numBytes bytes " +
                       s"-> $dataBytes (Long)")
          if (rows > batchSize) {
            throw new Exception(s"rows $rows > batchSize $batchSize")
          }
        case QflockServerHeader.DataType.DoubleType =>
           logger.trace(s"$tId:$id) read $numBytes bytes (Double)")
          val dataBytes = header.getInt(QflockServerHeader.Offset.dataLen)
          if (compressed) {
              val cb = new Array[Byte](numBytes)
              readFully(stream, cb, 0, numBytes)
              logger.trace(s"$tId:$id) decompressing (Double)")
              Zstd.decompress(byteBuffer.array(), cb)
          } else {
            readFully(stream, byteBuffer.array(), 0, dataBytes)
          }
          rows = dataBytes / 8
          fixedTextLen = 0
          logger.trace(s"$tId:$id) decompressed $numBytes " +
                      s"-> bytes $dataBytes (Double)")
          if (rows > batchSize) {
            throw new Exception(s"rows $rows > batchSize $batchSize")
          }
        case QflockServerHeader.DataType.FixedLenByteArrayType =>
          fixedTextLen = header.getInt(QflockServerHeader.Offset.typeSize)
          val dataBytes = header.getInt(QflockServerHeader.Offset.dataLen)
          if (dataBytes > bufferLength) {
            throw new Exception(s"dataBytes $dataBytes > bufferLength $bufferLength")
          }
          if (compressed) {
            val cb = new Array[Byte](numBytes)
            readFully(stream, cb, 0, numBytes)
            Zstd.decompress(byteBuffer.array(), cb)
          } else {
            readFully(stream, byteBuffer.array(), 0, dataBytes)
          }
          rows = dataBytes / fixedTextLen
        case QflockServerHeader.DataType.ByteArrayType =>
          val indexBytes = header.getInt(QflockServerHeader.Offset.dataLen)
          if (compressed) {
            // Read and decompress string index.
            val cb = new Array[Byte](numBytes)
            readFully(stream, cb, 0, numBytes)
            logger.trace(s"$tId:$id,$tId) read $numBytes bytes (String Index)")
            logger.trace(s"$tId:$id) decompressing (String Index)")
            Zstd.decompress(stringLen.array(), cb)
          } else {
            readFully(stream, stringLen.array(), 0, indexBytes)
          }
          rows = indexBytes / 4
          fixedTextLen = 0
          logger.trace(s"$tId:$id) decompressed $numBytes -> " +
                      s"bytes $indexBytes (String Index)")
          if (rows > batchSize) {
            throw new Exception(s"rows $rows > batchSize $batchSize")
          }
          var idx = 0
          for (i <- 0 until rows) {
            stringIndex(i) = idx
            idx += stringLen.getInt(i * 4)
          }
          readFully(stream, header.array(), 0, header.capacity())
          val compressedBytes = header.getInt(QflockServerHeader.Offset.compressedLen)
          val dataBytes = header.getInt(QflockServerHeader.Offset.dataLen)
          compressed = compressedBytes > 0
          if (compressed) {
            val cb = new Array[Byte](compressedBytes)
            readFully(stream, cb, 0, compressedBytes)
            logger.trace(s"$tId:$id) read $compressedBytes bytes (String)")
            logger.trace(s"$tId:$id) decompressing (String)")
            Zstd.decompress(byteBuffer.array(), cb)
          } else {
            readFully(stream, byteBuffer.array(), 0, dataBytes)
          }
          logger.trace(s"$tId:$id) decompressed $dataBytes bytes " +
                      s"-> $dataBytes (String)")
          if (dataBytes > bufferLength) {
            throw new Exception(s"textBytes $compressedBytes > bufferLength $bufferLength")
          }
      }
    } catch {
      case _: EOFException =>
        // logger.warn(ex.toString)
      case ex: Exception =>
        val sw = new StringWriter
        ex.printStackTrace(new PrintWriter(sw))
        logger.error(sw.toString)
        throw ex
    }
    rows
  }
}

object QflockRemoteColumnVector {
  var colIndex = 0
  /** Returns an array of CompactCompressedColumnVectors.
   *  Use of an CompactCompressedColumnVector is always in sets to represent
   *  batches of data.  Thus they are only useful in sets.
   *  This provides the api to return a relevant set of
   *  CompactCompressedColumnVectors representing the appropriate types.
   *
   *  @param batchSize the number of rows in a batch
   *  @param dataTypes the QflockServerHeader.DataType to use for each vector.
   *  @param schema the relevant schema for the vector.
   */
  def apply(batchSize: Integer,
            dataTypes: Array[Int],
            schema: StructType,
            cachedData: Option[QflockFileCachedData] = None): Array[QflockRemoteColumnVector] = {
    val vectors = new Array[QflockRemoteColumnVector](dataTypes.length)
    for (i <- 0 until dataTypes.length) {
      val id = f"$colIndex%d) $i%d/${dataTypes.length}%d"
      colIndex += 1
      vectors(i) = new QflockRemoteColumnVector(batchSize, dataTypes(i), schema, id,
        cachedData)
    }
    vectors
  }
}

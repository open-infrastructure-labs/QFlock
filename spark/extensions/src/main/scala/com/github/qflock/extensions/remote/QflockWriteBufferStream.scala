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

import java.io.{DataOutputStream, FileOutputStream}
import java.nio.ByteBuffer
import java.util.concurrent.ArrayBlockingQueue

import com.github.luben.zstd.Zstd
import com.github.qflock.server.{QflockDataStreamItem, QflockServerHeader}
import org.slf4j.LoggerFactory

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.types._

/** This is the object that contains data and streams it back to a client.
 *
 * @param schema the schema of write data
 * @param batchSize size of batch in rows to stream data
 * @param outputStream the stream to push data into.
 * @param pool - Back pointer to the pool from which we were allocated.
 */
class QflockWriteBufferStream(schema: StructType,
                              batchSize: Int,
                              outputStream: DataOutputStream,
                              pool: QflockWriteBufferPool)
    extends QflockDataStreamItem {
  private val logger = LoggerFactory.getLogger(getClass)
  var name: String = ""
  def setName(newName: String): Unit = {
    name = newName
  }

  override def toString: String = {
    name
  }
  // When the buffer gets full, such as when strings fill up the buffer,
  // this gets set in order to signal to client this buffer needs flush.
  var isFull: Boolean = false
  private var numBytes: Int = 0
  private val compressionLevel = 3
  private val bufferLength: Array[Int] = {
    schema.fields.map(x => batchSizeForType(x.dataType))
  }
  private val compressBuffers: Array[ByteBuffer] = {
    schema.fields.map(x => ByteBuffer.allocate(batchSizeForType(x.dataType)))
  }
  private val dataBuffers: Array[ByteBuffer] = {
    schema.fields.map(x => ByteBuffer.allocate(batchSizeForType(x.dataType)))
  }
  private def batchSizeForType(dataType: DataType): Int = {
    dataType match {
      case IntegerType => batchSize * 4
      case LongType => batchSize * 8
      case DoubleType => batchSize * 8
      case StringType => batchSize * 8 // QflockServerHeader.stringLength
    }
  }
  private def sizeForType(dataType: DataType): Int = {
    dataType match {
      case IntegerType => 4
      case LongType => 8
      case DoubleType => 8
      case StringType => QflockServerHeader.stringLength
    }
  }
  private val header = getHeader
  def getHeader: Array[ByteBuffer] = {
    val h = schema.fields.map(_ => ByteBuffer.allocate(QflockServerHeader.bytes))
    schema.fields.zipWithIndex.map(s => {
      s._1.dataType match {
        case LongType =>
          h(s._2).putInt(QflockServerHeader.Offset.dataType,
            QflockServerHeader.DataType.LongType.id)
          h(s._2).putInt(QflockServerHeader.Offset.typeSize,
            QflockServerHeader.Length.Long)
        case DoubleType =>
          h(s._2).putInt(QflockServerHeader.Offset.dataType,
            QflockServerHeader.DataType.DoubleType.id)
          h(s._2).putInt(QflockServerHeader.Offset.typeSize,
            QflockServerHeader.Length.Double)
        case StringType =>
          h(s._2).putInt(QflockServerHeader.Offset.dataType,
            QflockServerHeader.DataType.ByteArrayType.id)
          h(s._2).putInt(QflockServerHeader.Offset.typeSize, 0)
      }
    })
    h
  }
  val compressStringLengths: Array[ByteBuffer] = {
    schema.fields.map(x => x.dataType match {
      case StringType => ByteBuffer.allocate(4 * batchSize)
      case _ => ByteBuffer.allocate(0)
    })
  }
  val stringLengths: Array[ByteBuffer] = {
    schema.fields.map(x => x.dataType match {
      case StringType => ByteBuffer.allocate(4 * batchSize)
      case _ => ByteBuffer.allocate(0)
    })
  }
  private type ValueWriter = (SpecializedGetters, Int) => Unit
  // `ValueWriter`s for all fields of the schema
  private var fieldWriters: Array[ValueWriter] = _
  fieldWriters = schema.map(_.dataType).map(makeWriter).toArray[ValueWriter]

  private def makeWriter(dataType: DataType): ValueWriter = {
    // borrowed from (ParquetWriteSupport)
    dataType match {
      case ByteType =>
        (row: SpecializedGetters, ordinal: Int) =>
          dataBuffers(ordinal).putInt(row.getByte(ordinal))
      case ShortType =>
        (row: SpecializedGetters, ordinal: Int) =>
          dataBuffers(ordinal).putShort(row.getShort(ordinal))
      case IntegerType =>
        (row: SpecializedGetters, ordinal: Int) =>
          dataBuffers(ordinal).putInt(row.getInt(ordinal))
      case LongType =>
        (row: SpecializedGetters, ordinal: Int) =>
          dataBuffers(ordinal).putLong(row.getLong(ordinal))
      case FloatType =>
        (row: SpecializedGetters, ordinal: Int) =>
          dataBuffers(ordinal).putFloat(row.getFloat(ordinal))
      case DoubleType =>
        (row: SpecializedGetters, ordinal: Int) =>
          dataBuffers(ordinal).putDouble(row.getDouble(ordinal))
      case StringType =>
        (row: SpecializedGetters, ordinal: Int) =>
          val utfBytes = row.getUTF8String(ordinal).getBytes
          val currentBytes = utfBytes.length
          numBytes += currentBytes
          if (numBytes >= bufferLength(ordinal) - QflockServerHeader.stringLength) {
            // logger.info(s"buffer full ${bufferLength(ordinal)} ${toString}")
            isFull = true
          }
          stringLengths(ordinal).putInt(currentBytes)
          dataBuffers(ordinal).put(utfBytes)

      // TODO Adds IntervalType support
      case _ => sys.error(s"Unsupported data type $dataType.")
    }
  }
  def writeFields(row: InternalRow): Unit = {
    var i = 0
    while (i < row.numFields) {
      if (!row.isNullAt(i)) {
        fieldWriters(i).apply(row, i)
      }
      i += 1
    }
  }
  def free(): Unit = {
    pool.free(this)
  }
  private var rows: Int = 0
  def setRows(newRows: Int): Unit = rows = newRows
  def reset(): Unit = {
    isFull = false
    numBytes = 0
//    totalCompressedBytes = 0
//    totalUncompressedBytes = 0
  }
//  var totalCompressedBytes: Long = 0
//  var totalUncompressedBytes: Long = 0
  def process: Unit = {
    for (i <- Range(0, schema.fields.length)) {
      if (schema.fields(i).dataType != StringType) {
//        logger.trace(s"col $i rows $rows $name")
        val dataLen = rows * sizeForType(schema.fields(i).dataType)
        val compressedBytes = Zstd.compressByteArray(
          compressBuffers(i).array(), 0, compressBuffers(i).array().length,
          dataBuffers(i).array(), 0, dataLen, compressionLevel)
        if (false) {
          val fName = s"${schema.fields(i).name}_data.bin"
          val fos = new FileOutputStream(s"/qflock/spark/build/$fName", true)
          fos.write(dataBuffers(i).array(), 0, dataBuffers(i).position())
          fos.close()
          val fNameComp = s"${schema.fields(i).name}_compressed.bin"
          val fosComp = new FileOutputStream(s"/qflock/spark/build/$fNameComp", true)
          fosComp.write(compressBuffers(i).array(), 0, compressedBytes.toInt)
          fosComp.close()
        }
        header(i).putInt(QflockServerHeader.Offset.dataLen,
          dataLen)
        header(i).putInt(QflockServerHeader.Offset.compressedLen,
          compressedBytes.toInt)
        outputStream.write(header(i).array())
        // The buffer is larger than the amount we need to transfer, just
        // write the length of the compressed bytes.
//        totalUncompressedBytes += dataBuffers(i).position()
//        totalCompressedBytes += compressedBytes
        outputStream.write(compressBuffers(i).array(), 0, compressedBytes.toInt)
        outputStream.flush()
        dataBuffers(i).clear()
      } else { // Strings
//        logger.trace(s"str col $i rows $rows $name")
        // First compress and send lengths
        var compressedBytes = Zstd.compressByteArray(
          compressBuffers(i).array(), 0, compressBuffers(i).array().length,
          stringLengths(i).array(), 0, stringLengths(i).position(), compressionLevel)
        header(i).putInt(QflockServerHeader.Offset.dataLen,
          stringLengths(i).position())
        header(i).putInt(QflockServerHeader.Offset.compressedLen,
          compressedBytes.toInt)
        outputStream.write(header(i).array())
        // The buffer is larger than the amount we need to transfer, just
        // write the length of the compressed bytes.
        outputStream.write(compressBuffers(i).array(), 0, compressedBytes.toInt)
        stringLengths(i).clear()
        // Next compress and send strings
        compressedBytes = Zstd.compressByteArray(
          compressBuffers(i).array(), 0, compressBuffers(i).array().length,
          dataBuffers(i).array(), 0, dataBuffers(i).position(), compressionLevel)
        header(i).putInt(QflockServerHeader.Offset.dataLen,
          dataBuffers(i).position())
        header(i).putInt(QflockServerHeader.Offset.compressedLen,
          compressedBytes.toInt)
        outputStream.write(header(i).array())
        // The buffer is larger than the amount we need to transfer, just
        // write the length of the compressed bytes.
//        totalUncompressedBytes += dataBuffers(i).position()
//        totalCompressedBytes += compressedBytes
        outputStream.write(compressBuffers(i).array(), 0, compressedBytes.toInt)
        outputStream.flush()
        dataBuffers(i).clear()
      }
    }
  }
}


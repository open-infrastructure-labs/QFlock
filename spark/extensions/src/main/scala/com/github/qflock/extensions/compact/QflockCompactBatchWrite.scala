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

import java.io.{ByteArrayOutputStream, DataOutputStream, FileOutputStream, OutputStream}
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.ArrayBlockingQueue

import com.github.luben.zstd.Zstd
import com.github.qflock.server.{QflockDataStreamItem, QflockServerHeader}
import org.slf4j.LoggerFactory

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String





class QflockCompactBatchWrite(writeInfo: LogicalWriteInfo) extends BatchWrite {
  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo):
               DataWriterFactory = {
    val jdbcParams = new util.HashMap[String, String](writeInfo.options())
    new QflockCompactDataWriterFactory(writeInfo.schema(), jdbcParams)
  }

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    // nothing to do for single partition
  }

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
  }
}
class QflockCompactDataWriterFactory(schema: StructType, options: util.Map[String, String])
extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new QflockCompactDataWriter(partitionId, taskId, schema, options)
  }
}
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
  val compressionLevel = 3
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
      case StringType => batchSize * QflockServerHeader.stringLength
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
  private val strLenHeader = getHeader
  def getHeader: Array[ByteBuffer] = {
    val h = schema.fields.map(f => ByteBuffer.allocate(QflockServerHeader.bytes))
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
          stringLengths(ordinal).putInt(utfBytes.length)
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
  def free: Unit = {
    pool.free(this)
  }
  private var rows: Int = 0
  def setRows(newRows: Int): Unit = rows = newRows
  def reset: Unit = {
    totalCompressedBytes = 0
    totalUncompressedBytes = 0
  }
  var totalCompressedBytes: Long = 0
  var totalUncompressedBytes: Long = 0
  def process: Long = {
    for (i <- Range(0, schema.fields.length)) {
      if (schema.fields(i).dataType != StringType) {
//        logger.trace(s"col $i rows $rows $name")
        val dataLen = rows * sizeForType(schema.fields(i).dataType)
        val compressedBytes = Zstd.compressByteArray(
          compressBuffers(i).array(), 0, compressBuffers(i).array().length,
          dataBuffers(i).array(), 0, dataLen, compressionLevel)
        header(i).putInt(QflockServerHeader.Offset.dataLen,
          dataLen)
        header(i).putInt(QflockServerHeader.Offset.compressedLen,
          compressedBytes.toInt)
//        totalCompressedBytes += 16
//        totalUncompressedBytes += 16
        outputStream.write(header(i).array())
        // The buffer is larger than the amount we need to transfer, just
        // write the length of the compressed bytes.
        totalUncompressedBytes += dataBuffers(i).position()
        totalCompressedBytes += compressedBytes
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
//        totalCompressedBytes += header(i).position()
//        totalUncompressedBytes += header(i).position()
        outputStream.write(header(i).array())
        // The buffer is larger than the amount we need to transfer, just
        // write the length of the compressed bytes.
//        totalUncompressedBytes += stringLengths(i).position()
//        totalCompressedBytes += compressedBytes
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
//        totalCompressedBytes += header(i).position()
//        totalUncompressedBytes += header(i).position()
        outputStream.write(header(i).array())
        // The buffer is larger than the amount we need to transfer, just
        // write the length of the compressed bytes.
        totalUncompressedBytes += dataBuffers(i).position()
        totalCompressedBytes += compressedBytes
        outputStream.write(compressBuffers(i).array(), 0, compressedBytes.toInt)
        outputStream.flush()
        dataBuffers(i).clear()
      }
    }
    totalCompressedBytes
  }
}
case class QflockWriteBufferPool(count: Int,
                                 schema: StructType,
                                 stream: DataOutputStream,
                                 batchSize: Int) {
  val pool: ArrayBlockingQueue[QflockWriteBufferStream] = {
    val pool = new ArrayBlockingQueue[QflockWriteBufferStream](count)
    for (i <- 0 until count) {
      pool.add(new QflockWriteBufferStream(schema, batchSize, stream, this))
    }
    pool
  }
  def size: Int = pool.size()
  def allocate: QflockWriteBufferStream = {
    pool.take()
  }
  var totalCompressedBytes: Long = 0
  var totalUncompressedBytes: Long = 0
  def free(item: QflockWriteBufferStream): Unit = {
    totalCompressedBytes += item.totalCompressedBytes
    totalUncompressedBytes += item.totalUncompressedBytes
    item.reset
    pool.add(item)
  }
}
class QflockCompactDataWriter(partId: Int, taskId: Long,
                              schema: StructType,
                              options: util.Map[String, String])
      extends DataWriter[InternalRow] {

  private val logger = LoggerFactory.getLogger(getClass)
  private var rowIndex: Int = 0
  private val batchSize: Int = QflockServerHeader.batchSize

  // Client sets outStreamRequestId after calling fillRequestInfo
  private val requestId = options.get("outstreamrequestid").toInt
  private val streamDescriptor = QflockOutputStreamDescriptor.get.getRequestInfo(requestId)
//  if (streamDescriptor.wroteHeader != false) {
//    logger.info(s"$requestId has unexpected state of ${streamDescriptor.wroteHeader}")
//    throw new IllegalStateException("descriptor stat is not valid.")
//  }
  // The stream is used to write data back to the client.
  private val outputStream: DataOutputStream =
    streamDescriptor.stream.get.asInstanceOf[DataOutputStream]
  private val bufferPoolCount = 1
  val bufferPool = new QflockWriteBufferPool(bufferPoolCount, schema, outputStream, batchSize)
  var buffer = bufferPool.allocate
  writeDataFormat
  private def writeDataFormat: Unit = {
    // The data format consists of an int for magic,
    // an integer for number of columns,
    // followed by an integer for the type of each column.
    val buffer = ByteBuffer.allocate((schema.fields.length + 2) * 4)
    buffer.putInt(QflockServerHeader.magic)
    buffer.putInt(schema.fields.length)
    schema.fields.foreach(s => buffer.putInt(
      s.dataType match {
        case LongType => QflockServerHeader.DataType.LongType.id
        case DoubleType => QflockServerHeader.DataType.DoubleType.id
        case StringType => QflockServerHeader.DataType.ByteArrayType.id
      }
    ))
    val status = streamDescriptor.writeHeader(buffer)
//    val statusString = if (status) "yes" else "no"
//    logger.debug(s"$statusString " +
//                 s"for requestId $requestId partId/taskId $partId/$taskId " +
//                 s"${options.get("rgoffset")}/${options.get("rgcount")}")
  }
  private def setBufferName: Unit = {
    buffer.setName(s"partId/taskId $partId/$taskId rows $rowIndex totalRows $totalRows " +
      s"rg ${options.get("rgoffset")}/${options.get("rgcount")} ")
      // s"query ${options.get("query")}")
  }
  var totalRows = 0
  override def write(internalRow: InternalRow): Unit = {
    buffer.writeFields(internalRow)
    rowIndex += 1
    if (rowIndex >= batchSize) {
      totalRows += rowIndex
      // setBufferName
      buffer.setRows(rowIndex)
      streamDescriptor.streamAsync(buffer)
      buffer = bufferPool.allocate
      rowIndex = 0
    }
  }
  override def commit(): WriterCommitMessage = {
    new QflockCompactWriterCommitMessage(partId, taskId)
  }
  override def abort(): Unit = {}
  override def close(): Unit = {
    if (rowIndex > 0) {
      totalRows += rowIndex
      setBufferName
      buffer.setRows(rowIndex)
      streamDescriptor.streamAsync(buffer)
      rowIndex = 0
    } else {
      buffer.free
    }
    var loopCount = 0
    while (bufferPool.size < bufferPoolCount) {
      // logger.info(s"waiting for buffers to free $loopCount")
      loopCount += 1
      Thread.sleep(100)
    }
    if (loopCount > 0) {
      logger.info(s"done waiting for buffers to free $loopCount")
    }
    logger.info(s"rows $totalRows " +
                s"uncompressed ${bufferPool.totalUncompressedBytes} " +
                s"compressed ${bufferPool.totalCompressedBytes} ")
  }
}

class QflockCompactWriterCommitMessage(partId: Int, taskId: Long) extends WriterCommitMessage {

  def getPartitionId: Int = partId
  override def equals(obj: Any): Boolean = {
    if (this == obj) {
      true
    } else {
      if (!(obj.isInstanceOf[QflockCompactWriterCommitMessage])) {
        false
      } else {
        val msg = obj.asInstanceOf[QflockCompactWriterCommitMessage]
        partId == msg.getPartitionId
      }
    }
  }
  override def hashCode: Int = partId

  override def toString: String =
    "QflockCompactWriterCommitMessage(" + "partitionId=" + partId + " taskId=" + taskId + ')'

  def getTaskId: Long = taskId
}

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

import java.io.{BufferedOutputStream, ByteArrayOutputStream, DataOutputStream, FileOutputStream, OutputStream}
import java.nio.{ByteBuffer, ByteOrder}
import java.util
import java.util.Objects

import com.github.luben.zstd.{Zstd, ZstdOutputStream}
import com.github.qflock.extensions.remote.QflockOutputStreamDescriptor
import com.github.qflock.server.QflockServerHeader
import org.slf4j.LoggerFactory

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String



class QflockJdbcBatchWrite(writeInfo: LogicalWriteInfo) extends BatchWrite {
  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo):
               DataWriterFactory = {
    val jdbcParams = new util.HashMap[String, String](writeInfo.options())
    new QflockJdbcDataWriterFactory(writeInfo.schema(), jdbcParams)
  }

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    // nothing to do for single partition
  }

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
  }
}
class QflockJdbcDataWriterFactory(schema: StructType, options: util.Map[String, String])
extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new QflockJdbcDataWriter(partitionId, taskId, schema, options)
  }
}

class QflockJdbcDataWriter(partId: Int, taskId: Long,
                           schema: StructType,
                           options: util.Map[String, String])
      extends DataWriter[InternalRow] {

  private val logger = LoggerFactory.getLogger(getClass)
  logger.info(s"create partId: $partId taskId: $taskId")
  private var rowIndex: Int = 0
  private val batchSize: Int = 4096
  private def sizeForType(dataType: DataType): Int = {
    dataType match {
      case IntegerType => batchSize * 4
      case LongType => batchSize * 8
      case DoubleType => batchSize * 8
      case StringType => batchSize * QflockServerHeader.stringLength
    }
  }
  private val compressBuffers: Array[ByteBuffer] = {
    schema.fields.map(x => ByteBuffer.allocate(sizeForType(x.dataType)))
  }
  private val dataBuffers: Array[ByteBuffer] = {
    schema.fields.map(x => ByteBuffer.allocate(sizeForType(x.dataType)))
  }
  val stringLengths: Array[ByteBuffer] = {
    schema.fields.map(x => x.dataType match {
      case StringType => ByteBuffer.allocate(4 * batchSize)
      case _ => ByteBuffer.allocate(0)
    })
  }
  private type ValueWriter = (SpecializedGetters, Int) => Unit
  // `ValueWriter`s for all fields of the schema
  private var rootFieldWriters: Array[ValueWriter] = _
  rootFieldWriters = schema.map(_.dataType).map(makeWriter).toArray[ValueWriter]

  // Client sets outStreamRequestId after calling fillRequestInfo
  private val requestId = options.get("outstreamrequestid").toInt
  // The stream is used to write data back to the client.
  private val outputStream: DataOutputStream =
    QflockOutputStreamDescriptor.get.getRequestInfo(requestId)
      .stream.get.asInstanceOf[DataOutputStream]
  writeDataFormat
  private val query = options.get("query")
  private def writeDataFormat: Unit = {
    logger.info(s"write data format $query")
    // The data format consists of an integer for number of columns,
    // followed by an integer for the type of each column.
    val buffer = ByteBuffer.allocate((schema.fields.length + 1) * 4)
    buffer.putInt(schema.fields.length)
    schema.fields.foreach(s => buffer.putInt(
      s.dataType match {
        case LongType => QflockServerHeader.DataType.LongType.id
        case DoubleType => QflockServerHeader.DataType.DoubleType.id
        case StringType => QflockServerHeader.DataType.ByteArrayType.id
      }
    ))
    outputStream.write(buffer.array())
  }
  private val header = {
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
  private def writeFields(row: InternalRow,
                          fieldWriters: Array[ValueWriter]): Unit = {
    var i = 0
    while (i < row.numFields) {
      if (!row.isNullAt(i)) {
        fieldWriters(i).apply(row, i)
      }
      i += 1
    }
  }
  override def write(internalRow: InternalRow): Unit = {
    writeFields(internalRow, rootFieldWriters)
    rowIndex += 1
    if (rowIndex >= batchSize) {
      writeBuffers
    }
  }
  def writeBuffers: Unit = {
    if (rowIndex > 0) {
      for (i <- Range(0, schema.fields.length)) {
        if (schema.fields(i).dataType != StringType) {
          val compressedBytes = Zstd.compress(compressBuffers(i).array(),
            dataBuffers(i).array(), 1)
          header(i).putInt(QflockServerHeader.Offset.dataLen,
            dataBuffers(i).position())
          header(i).putInt(QflockServerHeader.Offset.compressedLen,
            compressedBytes.toInt)
          outputStream.write(header(i).array())
          // The buffer is larger than the amount we need to transfer, just
          // write the length of the compressed bytes.
          outputStream.write(compressBuffers(i).array(), 0, compressedBytes.toInt)
          outputStream.flush()
          //        Zstd.compressDirectByteBuffer(compressBuffers(x),
          //          0, bufferBytes,
          //          dataBuffers(x),
          //          0, bufferBytes, 1)
          dataBuffers(i).clear()
        } else { // Strings
          // First compress and send lengths
          var compressedBytes = Zstd.compress(compressBuffers(i).array(),
            stringLengths(i).array(), 1)
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
          compressedBytes = Zstd.compress(compressBuffers(i).array(),
                                          dataBuffers(i).array(), 1)
          header(i).putInt(QflockServerHeader.Offset.dataLen,
            dataBuffers(i).position())
          header(i).putInt(QflockServerHeader.Offset.compressedLen,
            compressedBytes.toInt)
          outputStream.write(header(i).array())
          // The buffer is larger than the amount we need to transfer, just
          // write the length of the compressed bytes.
          outputStream.write(compressBuffers(i).array(), 0, compressedBytes.toInt)
          outputStream.flush()
          dataBuffers(i).clear()
        }
      }
      rowIndex = 0
    }
  }
  override def commit(): WriterCommitMessage = {
    new QflockJdbcWriterCommitMessage(partId, taskId)
  }
  override def abort(): Unit = {}
  override def close(): Unit = {
    writeBuffers
  }
}

class QflockJdbcWriterCommitMessage(partId: Int, taskId: Long) extends WriterCommitMessage {

  def getPartitionId: Int = partId
  override def equals(obj: Any): Boolean = {
    if (this == obj) {
      true
    } else {
      if (!(obj.isInstanceOf[QflockJdbcWriterCommitMessage])) {
        false
      } else {
        val msg = obj.asInstanceOf[QflockJdbcWriterCommitMessage]
        partId == msg.getPartitionId
      }
    }
  }
  override def hashCode: Int = partId

  override def toString: String =
    "QflockJdbcWriterCommitMessage(" + "partitionId=" + partId + " taskId=" + taskId + ')'

  def getTaskId: Long = taskId
}

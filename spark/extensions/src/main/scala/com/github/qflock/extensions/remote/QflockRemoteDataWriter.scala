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

import java.io.DataOutputStream
import java.nio.ByteBuffer
import java.util

import com.github.qflock.server.QflockServerHeader
import org.slf4j.LoggerFactory

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types._

/** Object that handles accepting write data and redirecting it back to a client.
 *  We use the outstreamrequestid option to get access to the other parameters the
 *  caller wanted to pass to the data source.
 *
 * @param partId partition id being written
 * @param taskId task id of current context
 * @param schema the schema of the data to be written
 * @param options options of data source for the write.
 */
class QflockRemoteDataWriter(partId: Int, taskId: Long,
                             schema: StructType,
                             options: util.Map[String, String])
  extends DataWriter[InternalRow] {

  private val logger = LoggerFactory.getLogger(getClass)
  private var rowIndex: Int = 0
  private val batchSize: Int = QflockServerHeader.batchSize

  // Client sets outStreamRequestId after calling fillRequestInfo
  private val requestId = options.get("outstreamrequestid").toInt
  private val streamDescriptor = QflockOutputStreamDescriptor.get.getRequestInfo(requestId)
  // The stream is used to write data back to the client.
  private val outputStream: DataOutputStream =
    streamDescriptor.stream.get.asInstanceOf[DataOutputStream]
  private val bufferPoolCount = 2
  private val bufferPool = QflockWriteBufferPool(bufferPoolCount, schema, outputStream, batchSize)
  private var buffer = bufferPool.allocate
  writeDataFormat()
  private def writeDataFormat(): Unit = {
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
    streamDescriptor.writeHeader(buffer)
  }
  private def setBufferName(): Unit = {
    buffer.setName(s"partId/taskId $partId/$taskId rows $rowIndex totalRows $totalRows " +
      s"rg ${options.get("rgoffset")}/${options.get("rgcount")} ")
    // s"query ${options.get("query")}")
  }
  var totalRows = 0
  override def write(internalRow: InternalRow): Unit = {
    buffer.writeFields(internalRow)
    rowIndex += 1
    if (rowIndex >= batchSize || buffer.isFull) {
      totalRows += rowIndex
      // setBufferName
      buffer.setRows(rowIndex)
      streamDescriptor.streamAsync(buffer)
      buffer = bufferPool.allocate
      rowIndex = 0
    }
  }
  override def commit(): WriterCommitMessage = {
    new QflockRemoteWriterCommitMessage(partId, taskId)
  }
  override def abort(): Unit = {}
  override def close(): Unit = {
    if (rowIndex > 0) {
      totalRows += rowIndex
      setBufferName()
      buffer.setRows(rowIndex)
      streamDescriptor.streamAsync(buffer)
      rowIndex = 0
    } else {
      buffer.free()
    }
    var loopCount = 0
    while (bufferPool.size < bufferPoolCount) {
      // logger.info(s"waiting for buffers to free $loopCount")
      loopCount += 1
      Thread.sleep(100)
    }
    if (loopCount > 100) {
      logger.info(s"done waiting for buffers to free $loopCount")
    }
    //    logger.info(s"rows $totalRows " +
    //                s"uncompressed ${bufferPool.totalUncompressedBytes} " +
    //                s"compressed ${bufferPool.totalCompressedBytes} ")
  }
}

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

import java.io.{BufferedOutputStream, DataOutputStream, FileOutputStream}
import java.util

import com.github.qflock.extensions.common.{QflockCacheKeyEntry, QflockFileCachedData, QflockQueryCache}
import com.github.qflock.server.QflockServerHeader
import org.slf4j.LoggerFactory

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.vectorized.ColumnarBatch



/** Creates a factory for creating QflockCompactPartitionReaderFactory objects
 *
 * @param options the options including "path"
 */
class QflockCompactPartitionReaderFactory(options: util.Map[String, String],
                                          batchSize: Int = QflockServerHeader.batchSize)
  extends PartitionReaderFactory {
  private val logger = LoggerFactory.getLogger(getClass)

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new QflockCompactPartitionReader(options, partition.asInstanceOf[QflockCompactPartition])
  }

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val part = partition.asInstanceOf[QflockCompactPartition]
    val schema = QflockCompactDatasource.getSchema(options)
    val query = options.get("query")
    var cachedValue = QflockQueryCache.checkKey(query, part.index)

    val appId = options.get("appid")
//    val cachedDataEntry: Option[QflockFileCachedData] = None
    val cachedDataEntry: Option[QflockFileCachedData] = {
      if (cachedValue.isDefined) {
        val fileData = cachedValue.get.asInstanceOf[QflockFileCachedData]
        var waitCount = 0
        while(!fileData.isDataValid) {
          // Wait for the write of data to be complete.
          logger.warn(s" invalid-cached-data wait $waitCount" +
                      s"appId:$appId part:${part.index} key:$query")
          Thread.sleep(100)
          waitCount += 1
        }
        logger.warn(s" use-cached-data " + s"appId:$appId part:${part.index} key:$query")
        Some(fileData)
      } else {
        logger.warn(s" insert-cached-data " +
          s"appId:$appId part:${part.index} key:$query")
        QflockQueryCache.insertFileData(query, part.index)
      }
    }
    //    logger.info("QflockCompactPartitionReaderFactory creating partition " +
    //                s"part ${part.index} off ${part.offset} len ${part.length}")
    val client = {
      if (cachedValue.isDefined) {
        new QflockFileClient(cachedDataEntry.get.getFile)
      } else {
        new QflockCompactClient(query, part.name,
          part.offset.toString, part.length.toString,
          schema, options.get("url"))
      }
    }
//    logger.info("QflockCompactPartitionReaderFactory opened client " +
//                s"part ${part.index} off ${part.offset} len ${part.length}" +
//                s"query: " + options.get("query"))

    val reader = new QflockCompactColVectReader(schema, batchSize,
                                                query, client, cachedDataEntry)
    new QflockCompactColumnarPartitionReader(reader)
  }
}


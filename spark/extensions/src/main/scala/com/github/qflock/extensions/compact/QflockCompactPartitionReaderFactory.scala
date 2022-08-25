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

import java.util

import org.slf4j.LoggerFactory

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.vectorized.ColumnarBatch


/** Creates a factory for creating QflockCompactPartitionReaderFactory objects
 *
 * @param options the options including "path"
 */
class QflockCompactPartitionReaderFactory(options: util.Map[String, String],
                                          batchSize: Int = 4096)
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
    logger.info("QflockCompactPartitionReaderFactory created partition " + part.index)
    val client = new QflockCompactClient(query, schema, options.get("url"))
    logger.info("query: " + options.get("query"))
    val reader = new QflockCompactColVectReader(schema, batchSize, query, client)
    new QflockCompactColumnarPartitionReader(reader)
  }
}

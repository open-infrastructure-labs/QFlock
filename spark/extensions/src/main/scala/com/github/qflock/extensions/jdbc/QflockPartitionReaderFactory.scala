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

import java.util
import java.util.OptionalLong

import scala.collection.mutable.ArrayBuffer

import org.slf4j.LoggerFactory

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, Statistics => ReadStats, SupportsReportStatistics}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.util.SerializableConfiguration

/** Creates a factory for creating QflockPartitionReaderFactory objects
 *
 * @param options the options including "path"
 */
class QflockPartitionReaderFactory(options: util.Map[String, String],
      sharedConf: Broadcast[org.apache.spark.util.SerializableConfiguration])
  extends PartitionReaderFactory {
  private val logger = LoggerFactory.getLogger(getClass)
  private val sparkSession: SparkSession = SparkSession
    .builder()
    .getOrCreate()

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new QflockJdbcPartitionReader(options, partition.asInstanceOf[QflockJdbcPartition],
      sparkSession, sharedConf)
  }
  val batchSize = 1024

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val part = partition.asInstanceOf[QflockJdbcPartition]
    val schema = QflockJdbcDatasource.getSchema(options)
    val reader = new QflockJdbcVectReader(schema, 256 * 1024, part, options)
    logger.debug("QflockPartitionReaderFactory created row group " + part.index)
    new QflockJdbcColumnarPartitionReader(reader)
    // This alternate factory below is identical to the above, but
    // provides more verbose progress tracking.
    // new HdfsBinColColumnarPartitionReaderProgress(vectorizedReader, batchSize, part)
  }
}

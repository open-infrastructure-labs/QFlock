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

import org.slf4j.LoggerFactory

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, Statistics => ReadStats, SupportsReportStatistics}
import org.apache.spark.sql.vectorized.ColumnarBatch

/** PartitionReader which returns a ColumnarBatch, and
 *  is relying on the HdfsBinColVectReader to
 *  fetch the batches.
 *
 * @param vectorizedReader - Already initialized HdfsBinColVectReader
 *                           which provides the data for the PartitionReader.
 */
class QflockJdbcColumnarPartitionReader(vectorizedReader: QflockColVectReader)
  extends PartitionReader[ColumnarBatch] {
  private val logger = LoggerFactory.getLogger(getClass)
  override def next(): Boolean = vectorizedReader.next()
  override def get(): ColumnarBatch = {
    val batch = vectorizedReader.get.asInstanceOf[ColumnarBatch]
    batch
  }
  override def close(): Unit = vectorizedReader.close()
}



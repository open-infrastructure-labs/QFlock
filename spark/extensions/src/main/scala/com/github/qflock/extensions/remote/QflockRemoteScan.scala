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

import java.util
import java.util.OptionalLong

import scala.collection.mutable.ArrayBuffer

import org.slf4j.LoggerFactory

import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, Statistics => ReadStats, SupportsReportStatistics}
import org.apache.spark.sql.types._



/** A scan object that works on Jdbc.
 *
 * @param options the options including "path"
 */
case class QflockRemoteScan(schema: StructType,
                            options: util.Map[String, String],
                            statsParams: Option[Any] = None,
                            stats: Statistics = Statistics(0, Some(0)))
  extends Scan with Batch with SupportsReportStatistics {

  private val logger = LoggerFactory.getLogger(getClass)
  override def toBatch: Batch = this
  override def readSchema(): StructType = schema

  private var partitions: Array[InputPartition] = Array[InputPartition]()
  private val sizeInBytes: OptionalLong = OptionalLong.of(stats.sizeInBytes.longValue())
  private val rowCount: OptionalLong =
    OptionalLong.of(stats.rowCount.getOrElse(BigInt(0)).longValue())

  case class GenericPushdownStats(sizeInBytes: OptionalLong,
                                  numRows: OptionalLong) extends ReadStats

  override def estimateStatistics(): ReadStats = {
    // schema.defaultSize
    GenericPushdownStats(numRows = rowCount, sizeInBytes = sizeInBytes)
  }
  private def createPartitions(): Array[InputPartition] = {
    val tableName = options.get("tablename")
    val rowGroups = options.get("numrowgroups").toInt
    val batchSize: Int = options.getOrDefault("rowgroupbatchsize", "0").toInt
    val numRows = options.get("numrows").toInt
    // Set below to true to do a 1 partition test.
    val partitionArray = new ArrayBuffer[InputPartition](0)
//    if (path.contains("store_sales")) {
//      throw new Exception("fake exception")
//    }
    var partitions = 0
    if (false) {
      // generate one partition per batch.
      // last partition gets any remainder row groups
      for (i <- 0 until partitions) {
        partitionArray += new QflockRemotePartition(index = i,
                                                     offset = i,
                                                     length = 1,
                                                     name = tableName)
      }
    } else if (batchSize > 1) {
      partitions = {
        rowGroups / batchSize + (if ((rowGroups % batchSize) > 0) 1 else 0)
      }
      // generate one partition per batch.
      // last partition gets any remainder row groups
      for (i <- 0 until partitions) {
        val currentRowGroups = {
          if (i == partitions - 1) rowGroups - (batchSize * i)
          else batchSize
        }
        partitionArray += new QflockRemotePartition(index = i,
          offset = i * batchSize,
          length = currentRowGroups,
          name = tableName)
      }
    } else {
      val rowsPerPartition = numRows / rowGroups
      partitions = rowGroups
      // Generate one partition per row Group.
      for (i <- 0 until rowGroups) {
        partitionArray += new QflockRemotePartition(index = i,
          offset = i,
          length = 1,
          name = tableName,
          rows = rowsPerPartition)
      }
    }
    val query = options.get("query")
    val appId = options.get("appId")
    logger.info(s"Num partitions:$partitions app-id:$appId query:$query")
    logger.debug(partitionArray.mkString(", "))
    partitionArray.toArray
  }

  override def planInputPartitions(): Array[InputPartition] = {
    if (partitions.length == 0) {
      partitions = createPartitions()
    }
    partitions
  }
  override def createReaderFactory(): PartitionReaderFactory = {
      new QflockRemotePartitionReaderFactory(options)
  }
}


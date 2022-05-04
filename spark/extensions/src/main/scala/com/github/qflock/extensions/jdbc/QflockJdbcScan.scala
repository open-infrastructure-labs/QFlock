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
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, Statistics => ReadStats, SupportsReportStatistics}
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration


/** A scan object that works on Jdbc.
 *
 * @param options the options including "path"
 */
case class QflockJdbcScan(schema: StructType,
                          options: util.Map[String, String],
                          stats: Statistics = Statistics(0, Some(0)))
  extends Scan with Batch with SupportsReportStatistics {

  private val logger = LoggerFactory.getLogger(getClass)
  logger.trace("Created")
  override def toBatch: Batch = this
  override def readSchema(): StructType = schema

  private val maxPartSize: Long = (1024 * 1024 * 128)
  private var partitions: Array[InputPartition] = Array[InputPartition]()
  private var sizeInBytes: OptionalLong = OptionalLong.of(stats.sizeInBytes.longValue())
  private var rowCount: OptionalLong =
    OptionalLong.of(stats.rowCount.getOrElse(BigInt(0)).longValue())

  case class GenericPushdownStats(sizeInBytes: OptionalLong,
                                  numRows: OptionalLong) extends ReadStats

  override def estimateStatistics(): ReadStats = {
    // schema.defaultSize
    GenericPushdownStats(numRows = rowCount, sizeInBytes = sizeInBytes)
  }
  private def createPartitions(): Array[InputPartition] = {
    var a = new ArrayBuffer[InputPartition](0)
    val path = options.get("path")
//    val partitions = options.get("numRowGroups").toInt
    val partitions = 1
    logger.info(s"partitions ${partitions}")
    // Generate one partition per row Group.
    for (i <- 0 until partitions) {
      a += new QflockJdbcPartition(index = i,
        offset = i,
        length = 1,
        name = path)
//      a += new QflockJdbcPartition(index = i,
//        offset = 0,
//        length = 0,
//        name = path)
    }
    logger.info("Partitions: " + a.mkString(", "))
    a.toArray
  }
  private val sparkSession: SparkSession = SparkSession
    .builder()
    .getOrCreate()
  private val broadcastedHadoopConf =
    sparkSession.sparkContext.broadcast(new SerializableConfiguration(
      sparkSession.sessionState.newHadoopConf()))
  private val sqlConf = sparkSession.sessionState.conf

  override def planInputPartitions(): Array[InputPartition] = {
    if (partitions.length == 0) {
      partitions = createPartitions()
    }
    partitions
  }
  override def createReaderFactory(): PartitionReaderFactory = {
      new QflockPartitionReaderFactory(options, broadcastedHadoopConf)
  }
}

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
  logger.trace("Created")
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new QflockJdbcPartitionReader(options, partition.asInstanceOf[QflockJdbcPartition],
      sparkSession, sharedConf)
  }
}


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
package com.github.qflock.datasource.hdfs

import java.util

import scala.collection.mutable.ArrayBuffer

import com.github.qflock.datasource.common.Pushdown
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.BlockLocation
import org.apache.hadoop.fs.Path
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.slf4j.LoggerFactory

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.expressions.aggregate.{Aggregation => ExprAgg}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/** A scan object that works on HDFS files.
 *
 * @param schema the column format
 * @param options the options including "path"
 * @param filters the array of filters to push down
 * @param prunedSchema the new array of columns after pruning
 * @param pushedAggregation the array of aggregations to push down
 */
class HdfsScan(schema: StructType,
               options: util.Map[String, String],
               filters: Array[Filter], prunedSchema: StructType,
               pushedAggregation: Option[ExprAgg] = None)
      extends Scan with Batch {

  private val logger = LoggerFactory.getLogger(getClass)
  logger.trace("Created")

  private def init(): Unit = {
    if (options.get("format") == "parquet") {
      // The default for parquet is to use column names and no casts since
      // parquet knows what the data types are.
      options.put("useColumnNames", "")
      options.put("DisableCasts", "")
    }
  }
  init()
  override def toBatch: Batch = this

  protected val pushdown = new Pushdown(schema, prunedSchema, filters,
                                        pushedAggregation, options)

  override def readSchema(): StructType = pushdown.readSchema
  private val partitions: Array[InputPartition] = getPartitions

  private def createPartitionsParquet(blockMap: Map[String, Array[BlockLocation]]):
              Array[InputPartition] = {
    val a = new ArrayBuffer[InputPartition](0)
    val conf = new Configuration()
    val readOptions = HadoopReadOptions.builder(conf)
                                       .build()
    logger.trace("blockMap: {}", blockMap.mkString(", "))

    if (blockMap.size > 1) {
      // We do not handle it yet.
      throw new Exception(s"More than one file not yet handled. ${blockMap.size} files found")
    }
    val rowGroupOffset = options.get("rowGroupOffset").toInt
    val rowGroupCount = options.get("rowGroupCount").toInt
    // Generate one partition per file, per hdfs block
    for ((fName, _) <- blockMap) {
      val reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(fName),
        conf), readOptions)
      val parquetBlocks = reader.getFooter.getBlocks
      // Generate one partition per row Group.
      for (i <- rowGroupOffset until (rowGroupOffset + rowGroupCount)) {
        val parquetBlock = parquetBlocks.get(i)
        a += new HdfsPartition(index = i, offset = parquetBlock.getStartingPos,
          length = parquetBlock.getCompressedSize,
          name = fName,
          rows = parquetBlock.getRowCount,
          0,
          last = i == parquetBlocks.size - 1)
      }
    }
    logger.info(a.mkString(", "))
    a.toArray
  }
  private val sparkSession: SparkSession = SparkSession
      .builder()
      .getOrCreate()
  private val broadcastedHadoopConf = HdfsColumnarReaderFactory.getHadoopConf(sparkSession,
                                                                              pushdown.readSchema)
  private val sqlConf = sparkSession.sessionState.conf
  /** Returns an Array of Partitions for a given input file.
   *  the file is selected by options("path").
   *  If there is one file, then we will generate multiple partitions
   *  on that file if large enough.
   *  Otherwise we generate one partition per file based partition.
   *
   * @return array of Partitions
   */
  private def getPartitions: Array[InputPartition] = {
    val store: HdfsStore = HdfsStoreFactory.getStore(options, new Configuration())
    val fileName = store.filePath
    val blocks : Map[String, Array[BlockLocation]] = store.getBlockList(fileName)
    options.get("format") match {
      case "parquet" => createPartitionsParquet(blocks)
    }
  }
  override def planInputPartitions(): Array[InputPartition] = {
    partitions
  }
  override def createReaderFactory(): PartitionReaderFactory = {
    new HdfsColumnarPartitionReaderFactory(pushdown, options,
      broadcastedHadoopConf, sqlConf)
  }
}



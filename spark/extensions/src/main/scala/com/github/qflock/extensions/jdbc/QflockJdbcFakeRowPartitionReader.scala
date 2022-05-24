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

import org.slf4j.LoggerFactory

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader


/** Fake partitionreader that returns a single row.
 *
 * @param options the options including "path"
 * @param partition the QflockJdbcPartition to read from
 */
class QflockJdbcFakeRowPartitionReader(options: util.Map[String, String],
                                       partition: QflockJdbcPartition)
  extends PartitionReader[InternalRow] {

  private val logger = LoggerFactory.getLogger(getClass)
  private val numRows = partition.rows
  private val fakeRow: InternalRow = {
    if (options.getOrDefault("schema", "") != "") {
      InternalRow.fromSeq(options.get("schema").split(",").map(x => {
        val items = x.split(":")
        items(1) match {
          case "string" => "fake string"
          case "integer" => 42
          case "double" => 42.24
          case "long" => 42000.toLong
        }
      }))
    } else {
      InternalRow.empty
    }
  }
  var index = 0
  def next: Boolean = {
    if (index >= numRows) {
      logger.info(s"get: partition: ${partition.index} ${partition.offset}" +
        s" ${partition.length} ${partition.name} index: $index")
      false
    } else {
      true
    }
  }
  def get: InternalRow = {
    if ((index % 500000) == 0) {
      logger.info(s"get: partition: ${partition.index} ${partition.offset}" +
                  s" ${partition.length} ${partition.name} index: $index")
    }
    index = index + 1
    fakeRow
  }

  def close(): Unit = { }
}

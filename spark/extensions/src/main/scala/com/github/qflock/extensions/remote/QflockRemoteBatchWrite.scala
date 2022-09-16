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

import org.apache.spark.sql.connector.write._

/** Object provided to spark to realize a batch write that
 *  ends up with our own writer.
 *  Purpose here is to allow streaming this write data back to a client.
 *
 * @param writeInfo Contains options, schema, etc.
 */
class QflockRemoteBatchWrite(writeInfo: LogicalWriteInfo) extends BatchWrite {
  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo):
               DataWriterFactory = {
    val jdbcParams = new util.HashMap[String, String](writeInfo.options())
    new QflockRemoteDataWriterFactory(writeInfo.schema(), jdbcParams)
  }

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    // nothing to do for single partition
  }

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    // nothing to do for single partition
  }
}


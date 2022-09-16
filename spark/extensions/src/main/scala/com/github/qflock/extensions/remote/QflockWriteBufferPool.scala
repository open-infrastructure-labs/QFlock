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
import java.util.concurrent.ArrayBlockingQueue

import org.apache.spark.sql.types._

/** This is a pool of QflockWriteBufferStream objects.
 *
 * @param count Number of objects in pool
 * @param schema schema of write data
 * @param stream stream to be used to write data
 * @param batchSize size of batch in rows.
 */
case class QflockWriteBufferPool(count: Int,
                                 schema: StructType,
                                 stream: DataOutputStream,
                                 batchSize: Int) {
  val pool: ArrayBlockingQueue[QflockWriteBufferStream] = {
    val pool = new ArrayBlockingQueue[QflockWriteBufferStream](count)
    for (_ <- 0 until count) {
      pool.add(new QflockWriteBufferStream(schema, batchSize, stream, this))
    }
    pool
  }
  def size: Int = pool.size()
  def allocate: QflockWriteBufferStream = {
    pool.take()
  }
//  var totalCompressedBytes: Long = 0
//  var totalUncompressedBytes: Long = 0
  def free(item: QflockWriteBufferStream): Unit = {
//    totalCompressedBytes += item.totalCompressedBytes
//    totalUncompressedBytes += item.totalUncompressedBytes
    item.reset()
    pool.add(item)
  }
}


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

import com.github.qflock.extensions.common.SeekableByteArrayInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.log4j.Logger
import org.apache.parquet.hadoop.util.HadoopStreams
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream
import org.slf4j.LoggerFactory


/** An InputFile which can interface with the Ndp server.
 *
 *  @param store object for opening the stream.
 *  @param partition the partition to open.
 *
 */
class QflockBufferInputFile(buffer: Array[Byte]) extends InputFile {
  protected val logger = LoggerFactory.getLogger(getClass)
  var length: Long = 0
  override def getLength(): Long = {
    length
  }
  override def newStream(): SeekableInputStream = {
    HadoopStreams.wrap(new FSDataInputStream(
      new SeekableByteArrayInputStream(buffer)))
  }
  override def toString(): String = {
    "QflockBufferInputFile"
  }
}

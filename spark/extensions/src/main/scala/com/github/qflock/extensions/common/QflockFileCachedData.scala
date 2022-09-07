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
package com.github.qflock.extensions.common


import java.io.{BufferedOutputStream, DataOutputStream, File, FileOutputStream}

import org.slf4j.{Logger, LoggerFactory}

class QflockFileCachedData(key: String, partition: Int) {
  protected val logger: Logger = LoggerFactory.getLogger(getClass)
  private val generation = QflockFileCachedData.getGeneration
  private val cacheFile = s"/qflock/spark/build/cache/cache_data_${generation}_${partition}.bin"
  logger.info(s"stream cache gen:$generation file:$cacheFile " +
    s"part:${partition} key:$key")
  var stream: Option[DataOutputStream] = Some(new DataOutputStream(
    new BufferedOutputStream(
      new FileOutputStream(cacheFile))))
  def close: Unit = {
    if (stream.isDefined) {
      stream.get.flush()
      stream.get.close()
      stream = None
    }
  }
  def shouldWrite: Boolean = stream.isDefined
  def isDataValid: Boolean = stream.isEmpty
  def getFile: String = cacheFile
}

object QflockFileCachedData {
  protected val logger: Logger = LoggerFactory.getLogger(getClass)
  def init: Unit = {
    val dir = new File("/qflock/spark/build/cache")
    for (file <- dir.listFiles) {
      if (!file.isDirectory) {
        logger.info(s"Deleting: ${file.toString}")
        file.delete
      }
    }
  }
  var generation: Long = 0
  def getGeneration: Long = {
    synchronized {
      generation += 1
    }
    generation
  }
}

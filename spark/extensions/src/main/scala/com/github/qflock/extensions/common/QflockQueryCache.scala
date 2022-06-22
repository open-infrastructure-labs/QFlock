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

import org.slf4j.{Logger, LoggerFactory}


case class QflockCacheEntry(data: Any) {
  var hits = 0
  def incHits(): Unit = hits += 1
  def getData: Any = {
    incHits()
    data
  }
}
case class QflockCacheKeyEntry(query: String) {
  protected val logger: Logger = LoggerFactory.getLogger(getClass)
  private val cache = collection.mutable.Map[Int, QflockCacheEntry]()
  def insertData(partition: Int, newData: Any): Unit = {
    cache(partition) = QflockCacheEntry(newData)
  }
  def checkKey(partitionIndex: Int): Option[Any] = {
    val entry = cache.get(partitionIndex)
    if (entry.isDefined) {
      val value = Some(entry.get.getData)
      logger.info(s"Cache Hit part: $partitionIndex query: $query hits: ${entry.get.hits}")
      value
    } else {
      entry
    }
  }
  def checkAndRelease(partitionIndex: Int): Unit = {
    val entry = cache.get(partitionIndex)
    if (entry.isDefined && entry.get.hits == maxHits) {
      logger.info(s"Cache Release part: $partitionIndex " +
                  s"query: $query hits: ${entry.get.hits} maxHits: $maxHits")
      cache.remove(partitionIndex)
    }
  }
  var maxHits = 0
  def incMaxHits(): Unit = maxHits += 1
}
object QflockQueryCache {
  private val cache = collection.mutable.Map[String, QflockCacheKeyEntry]()

  def checkKey(key: String, partitionIndex: Int): Option[Any] = {
    this.synchronized {
      val entry = cache.get(key)
      if (entry.isDefined) {
        val data = entry.get.checkKey(partitionIndex)
        entry.get.checkAndRelease(partitionIndex)
        data
      } else {
        entry
      }
    }
  }
  def cacheNeeded(key: String): Boolean = {
    this.synchronized {
      val entry = cache.get(key)
      if (entry.isDefined) {
        entry.get.maxHits > 1
      } else {
        false
      }
    }
  }
  def addKey(key: String): Unit = {
    this.synchronized {
      val entry = cache.get(key)
      if (entry.isDefined) {
        cache(key).incMaxHits()
      } else {
        cache(key) = QflockCacheKeyEntry(key)
      }
    }
  }
  def insertData(key: String, partitionIndex: Int, data: Any): Boolean = {
    this.synchronized {
      val entry = cache.get(key)
      /* Only cache the queries that we know will get hits. */
      if (entry.isDefined && entry.get.maxHits > 0) {
        cache(key).insertData(partitionIndex, data)
        true
      } else {
        false
      }
    }
  }
}

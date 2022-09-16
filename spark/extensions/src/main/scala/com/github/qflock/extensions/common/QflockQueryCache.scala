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

import com.github.qflock.extensions.jdbc.QflockLog
import org.slf4j.{Logger, LoggerFactory}

/** Provides an entry/container for holding data in the cache.
 *
 * @param data
 * @param bytes
 */
case class QflockCacheEntry(data: Any, bytes: Int) {
  var hits = 0
  def incHits(): Unit = hits += 1
  def getData: Any = {
    incHits()
    data
  }
}

/** Provides a list of partitions for a given query.
 *  insertData() puts data into the cache for a specific query and partition.
 *  checkKey() looks up the partition for this query and returns any
 *             data object it finds
 * @param query
 */
case class QflockCacheKeyEntry(query: String) {
  protected val logger: Logger = LoggerFactory.getLogger(getClass)
  private val cache = collection.mutable.Map[Int, QflockCacheEntry]()
  var bytes: Long = 0
  def insertData(partition: Int, newData: Any, dataBytes: Int = 0): Unit = {
    bytes += dataBytes.toLong
    cache(partition) = QflockCacheEntry(newData, dataBytes)
  }
  def checkKey(partitionIndex: Int): Option[Any] = {
    val entry = cache.get(partitionIndex)
    if (entry.isDefined) {
      val value = Some(entry.get.getData)
      logger.info(s"Cache Hit part: $partitionIndex query: $query " +
        s"hits: ${entry.get.hits} bytes:$bytes")
      value
    } else {
      None
    }
  }
  def checkAndRelease(partitionIndex: Int): Unit = {
    val entry = cache.get(partitionIndex)
    if (entry.isDefined && entry.get.hits == maxHits) {
      bytes -= entry.get.bytes
      logger.info(s"Cache Release part: $partitionIndex " +
                  s"query:$query hits:${entry.get.hits} maxHits:$maxHits bytes:$bytes")
      cache.remove(partitionIndex)
    }
  }
  var maxHits = 0
  def incMaxHits(): Unit = maxHits += 1
}

/** Provides a caching service for sql queries.
 *
 * This service works by first engaging during spark planning.
 * Our custom spark rule will notify the cache of all the queries it has seen.
 * The rule will call addKey("query text") to inform the cache of all
 * queries it expects to issue.  This allows the cache to decide which specific
 * queries should be cached.  For instance, all queries with a certain
 * hit count (let's say 1, meaning that the query was seen twice) should be cached.
 *
 * Later when the query is actually issued, we use this early detection to know
 * if the query should be cached or not.
 */
object QflockQueryCache {
  protected val logger: Logger = LoggerFactory.getLogger(getClass)
  private var cache = collection.mutable.Map[String, QflockCacheKeyEntry]()
  private val maxBytes: Long = 1024L * 1024L * 1024L * 10L
  var bytes: Long = 0
  def checkKey(key: String, partitionIndex: Int): Option[Any] = {
    this.synchronized {
      val entry = cache.get(key)
      if (entry.isDefined) {
        val data = entry.get.checkKey(partitionIndex)
        // entry.get.checkAndRelease(partitionIndex)
        // updateBytes()
        data
      } else {
        entry
      }
    }
  }
  def getKey(key: String, partitionIndex: Int): Option[Any] = {
    this.synchronized {
      val entry = cache.get(key)
      if (entry.isDefined) {
        val data = entry.get.checkKey(partitionIndex)
        data
      } else {
        entry
      }
    }
  }

  /** puts information in our qflock log on the potentially cachable queries.
   *  This is very useful for understanding the picture of all cachable queries.
   * @param test - String identifying the test name.
   */
  def logPotentialHits(test: String): Unit = {
    for ((k, e) <- cache) {
      if (e.maxHits >= 1) {
        QflockLog.log(s"QflockQueryCache:logPotentialHits test:$test hits:${e.maxHits} query:$k")
      }
    }
    cache = collection.mutable.Map[String, QflockCacheKeyEntry]()
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

  /**
   *
   * @param key
   */
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

  def updateBytes(): Unit = {
    bytes = cache.map({ case (_, v) => v.bytes }).sum
  }

  /** Inserts actual data into the cache.
   *  We used this with our older "JDBC" api, but this is now deprecated
   *  in favor of caching the data in a file.
   *
   * @param key
   * @param partitionIndex
   * @param data
   * @param dataBytes
   * @return
   */
  @deprecated("this method is deprecated, use insertFileData instead", "version 4")
  def insertData(key: String, partitionIndex: Int, data: Any,
                 dataBytes: Int = 0): Boolean = {
    this.synchronized {
      val entry = cache.get(key)
      /* Only cache the queries that we know will get hits. */
      if (entry.isDefined && entry.get.maxHits > 0 && bytes < maxBytes) {
        cache(key).insertData(partitionIndex, data, dataBytes)
        bytes += dataBytes.toLong
        true
      } else {
        false
      }
    }
  }

  /** Creates an entry in the cache for query data saved in a file.
   *
   * @param key - The query.
   * @param partition - The partition for this query.
   * @return Optional QflockFileCachedData used to access the cached data.
   */
  def insertFileData(key: String, partition: Int): Option[QflockFileCachedData] = {
    var retVal: Option[QflockFileCachedData] = None
    this.synchronized {
      val entry = cache.get(key)
      /* Only cache the queries that we know will get hits. */
      if (entry.isDefined && entry.get.maxHits > 0) {
        val cachedDataEntry = new QflockFileCachedData(key, partition)
        cache(key).insertData(partition, cachedDataEntry)
        retVal = Some(cachedDataEntry)
      }
    }
    retVal
  }
}


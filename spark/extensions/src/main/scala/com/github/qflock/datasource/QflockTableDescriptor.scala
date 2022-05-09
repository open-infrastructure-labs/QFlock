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
package com.github.qflock.datasource

import scala.collection._
import scala.collection.JavaConverters._

import org.slf4j.LoggerFactory


case class QflockTableRecord(var offset: Int, var count: Int, var freed: Boolean) {

  def fill(off: Int, cnt: Int): Unit = {
    offset = off
    count = cnt
    freed = false
  }

  def free(): Unit = {
    freed = true
  }
}

case class QflockTableDescriptor(name: String, requests: Int) {
  private val requestMap = {

    val requestMap: concurrent.Map[Int, QflockTableRecord] =
      new java.util.concurrent.ConcurrentHashMap[Int, QflockTableRecord].asScala
    for (i <- 0 until requests) {
      requestMap(i) = QflockTableRecord(0, 0, freed = true)
    }
    requestMap
  }
  def getRequestInfo(requestId: Int): QflockTableRecord = {
    requestMap(requestId)
  }
  def fillRequestInfo(offset: Int, count: Int): Int = {
    val freeTuple = requestMap.find(_._2.freed == true)
    if (freeTuple.isEmpty) {
      throw new Exception("out of requests")
    }
    val (requestId, record) = freeTuple.get
    record.fill(offset, count)
    requestId
  }
  def freeRequest(requestId: Int): Unit = requestMap(requestId).free()
}

object QflockTableDescriptor {
  private val logger = LoggerFactory.getLogger(getClass)
  private val tableMap: concurrent.Map[String, QflockTableDescriptor] =
    new java.util.concurrent.ConcurrentHashMap[String, QflockTableDescriptor].asScala

  def addTable(tableName: String, maxRequests: Int): Unit = {
    tableMap(tableName) = QflockTableDescriptor(tableName, maxRequests)
  }

  def getTableDescriptor(tableName: String): QflockTableDescriptor = {
    tableMap(tableName)
  }
}

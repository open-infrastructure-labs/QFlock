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

import java.io.OutputStream

import scala.collection._
import scala.collection.JavaConverters._


case class QflockOutputStreamRecord(var stream: Option[OutputStream]) {

  def fill(strm: OutputStream): Unit = {
    stream = Some(strm)
  }

  def free(): Unit = {
    stream = None
  }
}

case class QflockOutputStreamDescriptor(requests: Int) {
  private val requestMap = {
    val requestMap: concurrent.Map[Int, QflockOutputStreamRecord] =
      new java.util.concurrent.ConcurrentHashMap[Int, QflockOutputStreamRecord].asScala
    for (i <- 0 until requests) {
      requestMap(i) = QflockOutputStreamRecord(None)
    }
    requestMap
  }
  def getRequestInfo(requestId: Int): QflockOutputStreamRecord = {
    requestMap(requestId)
  }
  def fillRequestInfo(stream: OutputStream): Int = {
    val freeTuple = requestMap.find(_._2.stream.isEmpty)
    if (freeTuple.isEmpty) {
      throw new Exception("out of requests")
    }
    val (requestId, record) = freeTuple.get
    record.fill(stream)
    requestId
  }
  def freeRequest(requestId: Int): Unit = requestMap(requestId).free()
}

/** This object holds the global state that allows us to
 *  pass objects as parameters to our data source.
 */
object QflockOutputStreamDescriptor {
  private val defaultRequests: Int = 16
  private var descriptor = new QflockOutputStreamDescriptor(defaultRequests)

  def initMap(maxRequests: Int): Unit = {
    descriptor = QflockOutputStreamDescriptor(maxRequests)
  }

  def get: QflockOutputStreamDescriptor = {
    descriptor
  }
}

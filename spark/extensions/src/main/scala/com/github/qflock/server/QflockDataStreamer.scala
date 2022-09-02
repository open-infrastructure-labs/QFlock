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
package com.github.qflock.server

import java.io.DataOutputStream
import java.lang.Thread
import java.util.concurrent.ArrayBlockingQueue

import org.slf4j.LoggerFactory

abstract class QflockDataStreamItem {
  def process: Long
  def free: Unit
}
class QflockDataStreamer extends java.lang.Thread {
  private val logger = LoggerFactory.getLogger(getClass)
  val queue: ArrayBlockingQueue[QflockDataStreamItem] =
    new ArrayBlockingQueue[QflockDataStreamItem](16)

  def enqueue(item: QflockDataStreamItem): Unit = {
//    logger.trace(s"item enqueued ${item.toString}")
    queue.put(item)
  }
  // Check if any streams are still in process of sending data.
  def streamsOutstanding: Boolean = (queue.size() > 0)
  def reset: Unit = bytesStreamed = 0
  var bytesStreamed: Long = 0
  override def run: Unit = {
    while (true) {
      val item = queue.take()
//      logger.trace(s"process item ${item.toString}")
      bytesStreamed += item.process
//      logger.trace(s"process item done ${item.toString}")
      item.free
    }
  }
}
object QflockDataStreamer {
  def apply(threads: Int): Array[QflockDataStreamer] = {
    Array.fill[QflockDataStreamer](threads)(new QflockDataStreamer)
  }
}

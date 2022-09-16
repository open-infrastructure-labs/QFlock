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


import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.channels.WritableByteChannel
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable

import com.github.qflock.server.QflockDataStreamer
import com.github.qflock.server.QflockDataStreamItem



class QflockOutputStreamRecord(var stream: Option[OutputStream]) {

  var channel: Option[WritableByteChannel] = None
  def updateStream(newOutputStream: OutputStream): Unit = {
    stream = Some(newOutputStream)
    channel = Some(Channels.newChannel(newOutputStream))
  }

  var freed: Boolean = true
  private val streamer: QflockDataStreamer = new QflockDataStreamer

  def fill(inputStream: OutputStream): Unit = {
    stream = Some(inputStream)
    updateStream(inputStream)
    freed = false
  }

  def free(): Unit = {
    stream = None
    wroteHeader = false
    freed = true
    streamer.reset
  }
  def bytesStreamed: Long = streamer.bytesStreamed
  def streamsOutstanding: Boolean = streamer.streamsOutstanding
  streamer.start()
  var wroteHeader: Boolean = false
  def writeHeader(byteBuffer: ByteBuffer): Boolean = {
    this.synchronized {
      if (!wroteHeader) {
        wroteHeader = true
        // channel.get.write(byteBuffer)
        stream.get.write(byteBuffer.array())
        stream.get.flush()
        true
      } else false
    }
  }
  def streamAsync(bufferStream: QflockDataStreamItem): Unit = {
    streamer.enqueue(bufferStream)
  }
}

case class QflockOutputStreamDescriptor(requests: Int) {
  private val freeQueue: ConcurrentLinkedQueue[Int] =
    new ConcurrentLinkedQueue[Int]()
  private val requestMap = {
    val requestMap: mutable.HashMap[Int, QflockOutputStreamRecord] =
      new mutable.HashMap[Int, QflockOutputStreamRecord]
    for (i <- 0 until requests) {
      requestMap(i) = new QflockOutputStreamRecord(None)
      freeQueue.add(i)
    }
    requestMap
  }
  def getRequestInfo(requestId: Int): QflockOutputStreamRecord = {
    requestMap(requestId)
  }
  def fillRequestInfo(stream: OutputStream): Int = {
    val requestId = freeQueue.remove()
    val record = requestMap(requestId)
    record.fill(stream)
    requestId
  }
  def freeRequest(requestId: Int): Unit = {
    val request = requestMap(requestId)
    request.free()
    freeQueue.add(requestId)
  }
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

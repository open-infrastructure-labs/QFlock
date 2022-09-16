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

import java.io.{BufferedInputStream, ByteArrayInputStream, DataInputStream, StringWriter}
import java.net.{HttpURLConnection, URL}
import java.nio.ByteBuffer
import javax.json.Json

import org.slf4j.LoggerFactory

import org.apache.spark.sql.types.StructType



class QflockRemoteClient(query: String,
                         tableName: String,
                         rgOffset: String,
                         rgCount: String,
                         schema: StructType,
                         urlPath: String) extends QflockClient {
  private val logger = LoggerFactory.getLogger(getClass)

  override def toString: String = {
    s"$tableName $rgOffset/$rgCount $query"
  }
  private def getJson(query: String): String = {
    val queryBuilder = Json.createObjectBuilder()
    queryBuilder.add("query", query)
    queryBuilder.add("tableName", tableName)
    queryBuilder.add("rgOffset", rgOffset)
    queryBuilder.add("rgCount", rgCount)
    val queryJson = queryBuilder.build
    val stringWriter = new StringWriter
    val writer = Json.createWriter(stringWriter)
    writer.writeObject(queryJson)
    stringWriter.getBuffer.toString
  }
  def getEmptyQueryStream(query: String, schema: StructType): DataInputStream = {
    // Write a header with a column number of 0.
    val b = ByteBuffer.allocate(4)
    b.putInt(0)
    val s = new DataInputStream(new ByteArrayInputStream(b.array()))
    s
  }
  private var connection: Option[HttpURLConnection] = None
  def close(): Unit = {
    if (connection.isDefined) {
//      logger.info("close start")
      stream.close()
      connection.get.disconnect()
//      logger.info("close end")
    }
  }
  private val stream = getQueryStream
  def getStream: DataInputStream = stream
  def getQueryStream: DataInputStream = {
//    logger.info(s"opening stream to: $tableName $rgOffset $rgCount")
    val url = new URL(urlPath)
    val con = url.openConnection.asInstanceOf[HttpURLConnection]
    connection = Some(con)
    con.setRequestMethod("POST")
    con.setRequestProperty("Accept", "application/json")
    con.setDoOutput(true)
    con.setDoInput(true)
    con.setReadTimeout(0)
    con.setConnectTimeout(0)
    con.connect()
    val jsonString = getJson(query)
    val os = con.getOutputStream
    try {
      val input = jsonString.getBytes("utf-8")
      os.write(input, 0, input.length)
    } finally if (os != null) os.close()
    val statusCode = con.getResponseCode
    if (statusCode == 200) {
//      logger.info(s"opening stream done $tableName $rgOffset $rgCount")
      new DataInputStream(new BufferedInputStream(con.getInputStream))
    } else {
      logger.error(s"unexpected http status on connect: $statusCode")
      getEmptyQueryStream(query, schema)
    }
  }
}

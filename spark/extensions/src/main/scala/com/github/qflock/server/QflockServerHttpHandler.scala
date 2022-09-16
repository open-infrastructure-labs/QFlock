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

import java.io._

import com.sun.net.httpserver.{HttpExchange, HttpHandler}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory


/** Is a handler of http requests for the Qflock Remote Server.
 *
 */
class QflockServerHttpHandler extends HttpHandler {
  private val logger = LoggerFactory.getLogger(getClass)
  def handle(httpExchange: HttpExchange): Unit = {
    if ("POST".equals(httpExchange.getRequestMethod)) {
      handlePostRequest(httpExchange)
    }
  }

  def handleGetRequest(httpExchange: HttpExchange): String = {
    httpExchange.getRequestURI
      .toString
      .split("\\?")(1)
      .split("=")(1)
  }
  def getRequestJson(stream: InputStream): Map[String, Any] = {
    val br = new BufferedReader(new InputStreamReader(stream, "utf-8"))
    val response = new StringBuilder
    try {
      var responseLine = br.readLine
      while (responseLine != null) {
        response.append(responseLine.trim)
        responseLine = br.readLine
      }
      logger.info(response.toString)
    } finally if (br != null) br.close()
    parse(response.toString).values.asInstanceOf[Map[String, Any]]
  }

  def handlePostRequest(httpExchange: HttpExchange): String = {
    val requestStream = httpExchange.getRequestBody
    val json = getRequestJson(requestStream)
    httpExchange.getResponseHeaders.set("Content-Type", "application/octet-stream")
    val outputStream = new DataOutputStream(
      new BufferedOutputStream(httpExchange.getResponseBody, 128 * 1024))
    // Set length to 0.  By convention this means that we will used chunked
    // format instead of content-length.  Close will end the response.
    httpExchange.sendResponseHeaders(200, 0)
    // Note that we pass the outputSteam to the QflockQueryHandler
    // The handler will eventually pass this stream along to our write data source
    // which will stream the data back to the client in a separate thread/data streamer.
    QflockQueryHandler.handleQuery(json("query").toString,
                                   json("tableName").toString,
                                   json("rgOffset").toString.toInt,
                                   json("rgCount").toString.toInt,
                                   outputStream)
    outputStream.write(QflockServerHeader.streamTerminator)
    outputStream.flush()
    // We close the stream to indicate that we are done.
    // This is a part of the api for chunked requests.
    httpExchange.close()
    ""
  }
}

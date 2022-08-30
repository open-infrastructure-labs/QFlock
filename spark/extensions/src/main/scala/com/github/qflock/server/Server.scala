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

import java.io.{BufferedOutputStream, BufferedReader, DataOutputStream, InputStream, InputStreamReader}
import java.net.InetSocketAddress
import java.util.concurrent.{Executors, ThreadPoolExecutor}

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.log4j.{Level, Logger}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory


class Server(hostName: String, port: Integer) {
  private val logger = LoggerFactory.getLogger(getClass)
  // Intentionally leave out the address to cause binding to all local adapters.
  private val server = HttpServer.create(new InetSocketAddress(port), 16)
  private val threadPoolExecutor = Executors.newFixedThreadPool(10)
  def start(): Unit = {
    QflockQueryHandler.init()
    Logger.getRootLogger().setLevel(Level.INFO)
    server.createContext("/test", new QflockHttpHandler())
    server.setExecutor(threadPoolExecutor)
    server.start()
    logger.info(s" Server started on port $port");
  }
}
class QflockHttpHandler extends HttpHandler {
  private val logger = LoggerFactory.getLogger(getClass)
  def handle(httpExchange: HttpExchange): Unit = {
    var requestParamValue = ""
    if ("GET".equals(httpExchange.getRequestMethod())) {
      requestParamValue = handleGetRequest(httpExchange)
      handleResponse(httpExchange, requestParamValue)
    } else if ("POST".equals(httpExchange.getRequestMethod())) {
      handlePostRequest(httpExchange)
    }
  }

  def handleGetRequest(httpExchange: HttpExchange): String = {
    return httpExchange.getRequestURI()
      .toString()
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
    val requestStream = httpExchange.getRequestBody()
    val json = getRequestJson(requestStream)
    val outputStream = new DataOutputStream(
      new BufferedOutputStream(httpExchange.getResponseBody(), 128*1024))
    // Set length to 0.  By convention this means that we will used chunked
    // format instead of content-length.  Close will end the response.
    httpExchange.sendResponseHeaders(200, 0)
    QflockQueryHandler.handleQuery(json("query").toString,
                                   json("tableName").toString,
                                   json("rgOffset").toString.toInt,
                                   json("rgCount").toString.toInt,
                                   outputStream)
//    logger.info(s"delay before closing stream")
//    Thread.sleep(5000)
    logger.debug(s"closing stream")
    httpExchange.close()
    logger.debug(s"stream closed")
    ""
  }
  def handleResponse(httpExchange: HttpExchange, requestParamValue: String) {
    val outputStream = httpExchange.getResponseBody()
    val htmlBuilder = new StringBuilder()
    htmlBuilder.append("<html>").
      append("<body>").
      append("<h1>").
      append("Hello ")
      .append(requestParamValue)
      .append("</h1>")
      .append("</body>")
      .append("</html>")
    // encode HTML content
    val htmlResponse = StringEscapeUtils.escapeHtml4(htmlBuilder.toString())
    // Use length if the length is known
    httpExchange.sendResponseHeaders(200, htmlResponse.length())
    // otherwise use zero and closing the outputstream
    // httpExchange.sendResponseHeaders(200, 0)

    outputStream.write(htmlResponse.getBytes())
    outputStream.flush()
    outputStream.close()
  }
  def handleResponseData(httpExchange: HttpExchange, requestParamValue: String) {
    val outputStream = httpExchange.getResponseBody()
    val htmlBuilder = new StringBuilder()
    htmlBuilder.append("<html>").
      append("<body>").
      append("<h1>").
      append("Hello ")
      .append(requestParamValue)
      .append("</h1>")
      .append("</body>")
      .append("</html>")
    // encode HTML content
    val htmlResponse = StringEscapeUtils.escapeHtml4(htmlBuilder.toString())
    // Use length if the length is known
    //    httpExchange.sendResponseHeaders(200, htmlResponse.length())
    // otherwise use zero and closing the outputstream
    httpExchange.sendResponseHeaders(200, 0)

    // outputStream.write(htmlResponse.getBytes())
    outputStream.flush()
    outputStream.close()
  }
}

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

import java.net.InetSocketAddress
import java.util.concurrent.Executors

import com.sun.net.httpserver.HttpServer
import org.apache.log4j.{ConsoleAppender, Level, Logger, PatternLayout}
import org.slf4j.LoggerFactory


/** This is the remote server object which is used to connect a
 *   remote spark instance to other external Spark clusters.
 *
 * @param hostName Name of the host to export/bind to
 * @param port port to bind to
 */
class QflockRemoteServer(hostName: String, port: Integer) {
  private val logger = LoggerFactory.getLogger(getClass)
  // Intentionally leave out the address to cause binding to all local adapters.
  private val server = HttpServer.create(new InetSocketAddress(port), 16)
  private val threadPoolExecutor = Executors.newFixedThreadPool(10)
  def start(): Unit = {
    QflockQueryHandler.init()
    Logger.getRootLogger.setLevel(Level.INFO)
    server.createContext("/query", new QflockServerHttpHandler())
    server.setExecutor(threadPoolExecutor)
    server.start()
    logger.info(s" Server started on port $port")
  }
}

/** This is a standalone server to be run to connect a
 *  remote spark instance with other spark instances.
 */
object QflockRemoteServer {
  def setupLogger(): Unit = {
    val console = new ConsoleAppender()
    val PATTERN = "%d{dd MMM yyyy HH:mm:ss.SSS [%p|%c|%C{1}] %m%n"
    console.setLayout(new PatternLayout(PATTERN))
    console.setThreshold(Level.INFO)
    console.activateOptions()
    Logger.getRootLogger.addAppender(console)
  }
  def main(args: scala.Array[String]): Unit = {
    val server = new QflockRemoteServer("qflock-spark-dc2", 9860)
    server.start()
  }
}

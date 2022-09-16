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

import java.io.{EOFException, OutputStream, PrintWriter, StringWriter}

import com.github.qflock.extensions.remote.QflockOutputStreamDescriptor
import org.slf4j.LoggerFactory

import org.apache.spark.sql.SparkSession

/** Handles queries for the server.
 *  This object has a set of views it instantiates at init time.
 *  These temp views are used for spark's spark.sql() call, which
 *  requires view names in order to reference tables.
 *
 *
 */
object QflockQueryHandler {
  private val logger = LoggerFactory.getLogger(getClass)
  private val spark = getSparkSession
  private val dbName = "tpcds"
  private val tables = QflockServerTable.getAllTables
  private val tablesMap = tables.map(t => t.getTableName -> t).toMap
  def init(): Unit = {
    tables.foreach(t => t.createViews())
  }
  private def getSparkSession: SparkSession = {
    logger.info(s"create new session")
    SparkSession
      .builder
      .master("local[4]")
      .appName("qflock-jdbc")
      .config("spark.local.dir", "/tmp/spark-temp")
      .enableHiveSupport()
      .getOrCreate()
  }
  // Initialize to use the database which contains our tables.
  spark.sql(s"USE $dbName")

  // We are using log4j.properties to control the log level.
  //  spark.sparkContext.setLogLevel("WARN")

  /** performs the spark query and instructs our
   *  write data source to ship the data back to our output stream.
   * @param query String representation of the query.
   * @param tableName name of the table to partition on
   * @param offset row group offset to start at
   * @param count number of row groups.
   * @param outStream stream of data to send data back to.
   * @return
   */
  def handleQuery(query: String,
                  tableName: String,
                  offset: Int,
                  count: Int,
                  outStream: OutputStream): String = {
    // When we handle a query we are issuing a spark query, where the
    // input data source is our data source (readRequestId) and the output
    // data source is our data source also (writeRequestId).
    // The request Ids are used to ship specific parameters to our data source including
    // for the read data source, the row group offset and row group count
    // for the write data source, the output data stream.
    // Note that the output stream descriptor also has embedded in it, a
    // QflockDataStreamer, which will be  used by that write in order to
    // stream data back to the client in a separate thread.
    val writeRequestId = QflockOutputStreamDescriptor.get.fillRequestInfo(outStream)
    val readRequestId = tablesMap(tableName).descriptor.fillRequestInfo(offset, count)
    val desc = QflockOutputStreamDescriptor.get.getRequestInfo(writeRequestId)
    if (desc.wroteHeader) {
      throw new IllegalStateException("descriptor stat is not valid.")
    }
    val newQuery = query.replace(s" $tableName",
                     s" ${tableName}_$readRequestId")
    logger.info(s"Start readRequestId: $readRequestId writeRequestId: $writeRequestId " +
      s"query: $newQuery")
    try {
      val df = spark.sql(newQuery)
      df // .repartition(1)
        // .orderBy((df.columns.toSeq map { x => col(x) }).toArray: _*)
        .write.format("qflockRemote")
        .mode("overwrite")
        .option("outStreamRequestId", writeRequestId)
        .option("rgoffset", offset)
        .option("rgcount", count)
        .option("query", newQuery)
        .save()
    } catch {
      case _: EOFException =>
      // logger.warn(ex.toString)
      case ex: Exception =>
        logger.error(s"error during query: $newQuery " +
                     s"rgoffset $offset rgcount $count" +
                     s"outStreamRequestId $writeRequestId")
        val sw = new StringWriter
        ex.printStackTrace(new PrintWriter(sw))
        logger.error(sw.toString)
        throw ex
    }
    var pollCount = 0
    // We do not want to allow the request to return until
    // after we have finished streaming back the data.
    // If there is a stream still outstanding, then we will
    // wait for it to complete.
    while (desc.streamsOutstanding) {
      logger.debug(s"Streams still outstanding $pollCount")
      Thread.sleep(10)
      pollCount += 1
    }
    if (pollCount > 0) {
      logger.info(s"streams outstanding pollCount $pollCount")
    }
    val bytesStreamed = desc.bytesStreamed
    QflockOutputStreamDescriptor.get.freeRequest(writeRequestId)
    tablesMap(tableName).descriptor.freeRequest(readRequestId)
    logger.info(s"Done readRequestId: $readRequestId " +
                s"writeRequestId: $writeRequestId " +
                s"bytesStreamed: $bytesStreamed")
    ""
  }
}

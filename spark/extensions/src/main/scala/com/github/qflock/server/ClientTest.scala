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

import java.io.{BufferedInputStream, BufferedReader, BufferedWriter, DataInputStream, File, FileOutputStream, InputStreamReader, PrintWriter, StringWriter}
import java.net.{HttpURLConnection, URL}
import javax.json.Json
import javax.json.JsonArrayBuilder
import javax.json.JsonObject
import javax.json.JsonObjectBuilder
import javax.json.JsonWriter

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.util.Success

import com.github.qflock.extensions.compact.{QflockCompactClient, QflockCompactColVectReader}
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.log4j.BasicConfigurator
import org.slf4j.LoggerFactory

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.extension.ExtHiveUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.ThreadUtils




class ClientTests {
  private val logger = LoggerFactory.getLogger(getClass)

  def getJson(query: String): String = {
    val queryBuilder = Json.createObjectBuilder()
    queryBuilder.add("query", query)
    val queryJson = queryBuilder.build
    val stringWriter = new StringWriter
    val writer = Json.createWriter(stringWriter)
    writer.writeObject(queryJson)
    stringWriter.getBuffer().toString()
  }

  def getSparkSession(): SparkSession = {
    logger.info(s"create new session")
    SparkSession
      .builder
      .master("local")
      .appName("qflock-jdbc")
      .config("spark.local.dir", "/tmp/spark-temp")
      .enableHiveSupport()
      .getOrCreate()
  }

  val spark = getSparkSession()
  spark.sparkContext.setLogLevel("INFO")

  def runQuery(query: String, tableName: String, rgOffset: String, rgCount: String,
               schema: StructType): ListBuffer[String] = {
    val url = "http://192.168.64.3:9860/test"
    val client = new QflockCompactClient(query, tableName,
      rgOffset, rgCount, schema, url)
    var data = ListBuffer.empty[String]
    try {
      data = readData(schema, 4096, client)
    } finally client.close
    data
  }

  def readData(schema: StructType,
               batchSize: Int,
               client: QflockCompactClient): ListBuffer[String] = {
    val data: ListBuffer[String] = ListBuffer.empty[String]
    val reader = new QflockCompactColVectReader(schema, batchSize, "", client)
    while (reader.next()) {
      val batch = reader.get()
      val rowIter = batch.rowIterator()
      while (rowIter.hasNext()) {
        val row = rowIter.next()
        val values = schema.fields.zipWithIndex.map(s => s._1.dataType match {
          case LongType => row.getLong(s._2)
          case DoubleType => row.getDouble(s._2)
          case StringType => row.getUTF8String(s._2)
        })
        // scalastyle:off println
        data += values.mkString(",")
        // scalastyle:on println
      }
    }
    reader.close()
    data
  }

  def queryCallCenter: Unit = {
    logger.info("start call_center")
    val schema = StructType(Array(
      StructField("cc_call_center_sk", LongType, true),
      StructField("cc_call_center_id", StringType, true),
      StructField("cc_rec_start_date", StringType, true),
      StructField("cc_rec_end_date", StringType, true),
      StructField("cc_closed_date_sk", LongType, true),
      StructField("cc_open_date_sk", LongType, true),
      StructField("cc_name", StringType, true),
      StructField("cc_class", StringType, true),
      StructField("cc_employees", LongType, true),
      StructField("cc_sq_ft", LongType, true),
      StructField("cc_hours", StringType, true),
      StructField("cc_manager", StringType, true),
      StructField("cc_mkt_id", LongType, true),
      StructField("cc_mkt_class", StringType, true),
      StructField("cc_mkt_desc", StringType, true),
      StructField("cc_market_manager", StringType, true),
      StructField("cc_division", LongType, true),
      StructField("cc_division_name", StringType, true),
      StructField("cc_company", LongType, true),
      StructField("cc_company_name", StringType, true),
      StructField("cc_street_number", StringType, true),
      StructField("cc_street_name", StringType, true),
      StructField("cc_street_type", StringType, true),
      StructField("cc_suite_number", StringType, true),
      StructField("cc_city", StringType, true),
      StructField("cc_county", StringType, true),
      StructField("cc_state", StringType, true),
      StructField("cc_zip", StringType, true),
      StructField("cc_country", StringType, true),
      StructField("cc_gmt_offset", DoubleType, true),
      StructField("cc_tax_percentage", DoubleType, true)
    ))
    runQueryTpcds("select * from call_center",
      "call_center", schema)
  }

  def queryWebReturns: Unit = {
    logger.info("start web_returns")
    val schema = StructType(Array(
      StructField("wr_returned_date_sk", LongType, true),
      StructField("wr_returned_time_sk", LongType, true),
      StructField("wr_item_sk", LongType, true),
      StructField("wr_refunded_customer_sk", LongType, true),
      StructField("wr_refunded_cdemo_sk", LongType, true),
      StructField("wr_refunded_hdemo_sk", LongType, true),
      StructField("wr_refunded_addr_sk", LongType, true),
      StructField("wr_returning_customer_sk", LongType, true),
      StructField("wr_returning_cdemo_sk", LongType, true),
      StructField("wr_returning_hdemo_sk", LongType, true),
      StructField("wr_returning_addr_sk", LongType, true),
      StructField("wr_web_page_sk", LongType, true),
      StructField("wr_reason_sk", LongType, true),
      StructField("wr_order_number", LongType, true),
      StructField("wr_return_quantity", LongType, true),
      StructField("wr_return_amt", DoubleType, true),
      StructField("wr_return_tax", DoubleType, true),
      StructField("wr_return_amt_inc_tax", DoubleType, true),
      StructField("wr_fee", DoubleType, true),
      StructField("wr_return_ship_cost", DoubleType, true),
      StructField("wr_refunded_cash", DoubleType, true),
      StructField("wr_reversed_charge", DoubleType, true),
      StructField("wr_account_credit", DoubleType, true),
      StructField("wr_net_loss", DoubleType, true)
    ))
    runQueryTpcds("select * from web_returns",
      "web_returns", schema)
  }

  def writeToFile(data: ListBuffer[String],
                  schema: StructType,
                  fileName: String): Unit = {
    val tmpFilename = fileName
    val writer = new PrintWriter(new FileOutputStream(
      new File(tmpFilename), true /* append */))
    data.foreach(x => writer.write(x + "\n"))
    writer.close()
  }

  def runQueryTpcds(query: String,
                    tableName: String,
                    schema: StructType): Unit = {
    val fileName = s"$tableName.csv"
    runQueryRowGroups(query, "tpcds",
      tableName, schema, fileName)
  }

  def runQueryRowGroups(query: String,
                        dbName: String,
                        tableName: String,
                        schema: StructType,
                        fileName: String): Unit = {
    val file = new java.io.File(fileName)
    if (file.exists) file.delete()
    val table: Table = ExtHiveUtils.getTable(dbName, tableName)
    val rgParamName = s"spark.qflock.statistics.tableStats.$tableName.row_groups"
    val numRowGroups = table.getParameters.get(rgParamName).toInt
    if (true) {
      logger.info(s"fetch row group 0-${numRowGroups - 1} table $dbName:$tableName")
      val data = runQuery(query, tableName, 0.toString, numRowGroups.toString, schema)
      writeToFile(data, schema, fileName)
    } else {
      for (i <- Range(0, numRowGroups)) {
        logger.info(s"fetch row group $i/${numRowGroups - 1} table $dbName:$tableName")
        val data = runQuery(query, tableName, i.toString, "1", schema)
        writeToFile(data, schema, fileName)
      }
    }
  }

  def queryStoreSales: Unit = {
    logger.info("start store_sales")
    val schema = StructType(Array(
      StructField("ss_sold_date_sk", LongType, true),
      StructField("ss_sold_time_sk", LongType, true),
      StructField("ss_item_sk", LongType, true),
      StructField("ss_customer_sk", LongType, true),
      StructField("ss_cdemo_sk", LongType, true),
      StructField("ss_hdemo_sk", LongType, true),
      StructField("ss_addr_sk", LongType, true),
      StructField("ss_store_sk", LongType, true),
      StructField("ss_promo_sk", LongType, true),
      StructField("ss_ticket_number", LongType, true),
      StructField("ss_quantity", LongType, true),
      StructField("ss_wholesale_cost", DoubleType, true),
      StructField("ss_list_price", DoubleType, true),
      StructField("ss_sales_price", DoubleType, true),
      StructField("ss_ext_discount_amt", DoubleType, true),
      StructField("ss_ext_sales_price", DoubleType, true),
      StructField("ss_ext_wholesale_cost", DoubleType, true),
      StructField("ss_ext_list_price", DoubleType, true),
      StructField("ss_ext_tax", DoubleType, true),
      StructField("ss_coupon_amt", DoubleType, true),
      StructField("ss_net_paid", DoubleType, true),
      StructField("ss_net_paid_inc_tax", DoubleType, true),
      StructField("ss_net_profit", DoubleType, true)
    ))
    runQueryTpcds("select * from store_sales",
      "store_sales", schema)
  }

  def queryItem: Unit = {
    logger.info("start item")
    val schema = StructType(Array(
      StructField("i_item_sk", LongType, true),
      StructField("i_item_id", StringType, true),
      StructField("i_rec_start_date", StringType, true),
      StructField("i_rec_end_date", StringType, true),
      StructField("i_item_desc", StringType, true),
      StructField("i_current_price", DoubleType, true),
      StructField("i_wholesale_cost", DoubleType, true),
      StructField("i_brand_id", LongType, true),
      StructField("i_brand", StringType, true),
      StructField("i_class_id", LongType, true),
      StructField("i_class", StringType, true),
      StructField("i_category_id", LongType, true),
      StructField("i_category", StringType, true),
      StructField("i_manufact_id", LongType, true),
      StructField("i_manufact", StringType, true),
      StructField("i_size", StringType, true),
      StructField("i_formulation", StringType, true),
      StructField("i_color", StringType, true),
      StructField("i_units", StringType, true),
      StructField("i_container", StringType, true),
      StructField("i_manager_id", LongType, true),
      StructField("i_product_name", StringType, true)
    ))
    runQueryTpcds("select * from item",
      "item", schema)
  }
}
object ClientTest {
  private val logger = LoggerFactory.getLogger(getClass)
  def parallelTests: Unit = {
    val f0: Future[Int] = Future {
      logger.info("starting f0")
      val ct = new ClientTests
      ct.queryItem
      logger.info("finished f0")
      5
    }
    f0.onComplete {
      case Success(stat) => logger.info(s"f0 Completed with status $stat")
      case _ => logger.info("f0 completed with error")
    }
    val f1: Future[Int] = Future {
      logger.info("starting f1")
      val ct = new ClientTests
      ct.queryStoreSales
      logger.info("finished f1")
      6
    }
    f1.onComplete {
      case Success(stat) => logger.info(s"f1 Completed with status $stat")
      case _ => logger.info("f1 completed with error")
    }
    // scalastyle:off awaitresult
    Await.result(f0, 50.seconds)
    Await.result(f1, 50.seconds)
    // scalastyle:on awaitresult
  }
  def main(args: scala.Array[String]): Unit = {
    BasicConfigurator.configure
    val ct = new ClientTests
    val testName = if (args.length == 0) "call_center" else args(0)
    testName match {
      case "call_center" => ct.queryCallCenter
      case "web_returns" => ct.queryWebReturns
      case "item" => ct.queryItem
      case "store_sales" => ct.queryStoreSales
      case test@_ => logger.warn("Unknown test " + test)
    }
  }
}

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
package com.github.qflock

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.ByteBuffer
import java.nio.file.{Files, Paths}

import com.github.luben.zstd.{Zstd, ZstdOutputStream}
import org.slf4j.{Logger, LoggerFactory}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._



object QflockTest {
  protected val logger: Logger = LoggerFactory.getLogger(getClass)
  private val spark = SparkSession
    .builder()
    .appName("test")
    .getOrCreate()
 //  spark.sparkContext.setLogLevel("INFO")
  import spark.implicits._
  def log(msg: String): Unit = {
    print(msg + "\n")
  }
  final def test(table: String): Unit = {
    val tablePath = s"hdfs://qflock-storage-dc2:9000/tpcds-parquet-100g/${table}.parquet"
    log("Starting")
    val df0 = spark.read.format("parquet").load(tablePath)
    //    val df1 = df0.select("cc_call_center_sk")
    //    val df1 = df0.select("cc_call_center_id")
    val df1 = df0.select("wr_return_amt")
    //    val df1 = df0.select("wr_returned_date_sk")
    //    df1.show(1000, false)
    val values = new Array[Array[Any]](df1.schema.fields.length)
    val bytes = new Array[Array[Byte]](df1.schema.fields.length)
    val compressed = new Array[Array[Byte]](df1.schema.fields.length)
    var startTime = System.nanoTime()
    log(df1.schema.toString)
    val cols = df1.schema.fields.length
    for (a <- Range(0, cols)) {
      // log(s"get row $a " + df1.schema.fields(a).name)
      import spark.implicits._
      values(a) = (df1.schema.fields(a).dataType match {
        case LongType => df1.select(df1.schema.fields(a).name)
          .map(f => f.getLong(0))
        case IntegerType => df1.select(df1.schema.fields(a).name)
          .map(f => f.getInt(0))
        case FloatType => df1.select(df1.schema.fields(a).name)
          .map(f => f.getFloat(0))
        case DoubleType => df1.select(df1.schema.fields(a).name)
          .map(f => f.getDouble(0))
        case StringType => df1.select(df1.schema.fields(a).name)
          .map(f => f.getString(0))
      }).collect.toArray
    }
    var endTime = System.nanoTime()
    log(s"cols: ${cols} " +
      s"rows: ${values(0).length} " +
      s"elapsed: " + (endTime - startTime) / 1e6d)
    startTime = System.nanoTime()
    var totalBytes = 0
    for (a <- Range(0, cols)) {
       log(s"get Bytes $a")
      bytes(a) = {
        val baos = new ByteArrayOutputStream()
        //        val zstdStream = new ZstdOutputStream(baos)
        val out = new DataOutputStream(baos)
        values(a).foreach(x => df1.schema.fields(a).dataType match {
          case LongType => out.writeLong(x.asInstanceOf[Long])
          case IntegerType => out.writeInt(x.asInstanceOf[Int])
          case FloatType => out.writeFloat(x.asInstanceOf[Float])
          case DoubleType => out.writeDouble(x.asInstanceOf[Double])
          case StringType => out.writeUTF(x.asInstanceOf[String])
        })
        baos.toByteArray()
      }
      totalBytes += bytes(a).length
      log(s"$a) bytes: ${bytes(a).length} " + values(a)(0))
    }
    endTime = System.nanoTime()
    log(s"cols: ${cols} " +
      s"totalBytes: $totalBytes elapsed: " + (endTime - startTime) / 1e6d)
    if (true) {
      startTime = System.nanoTime()
      totalBytes = 0
      var totalCompBytes = 0
      for (a <- Range(0, cols)) {
        compressed(a) = Zstd.compress(bytes(a), 1)
        totalBytes += bytes(a).length
        totalCompBytes += compressed(a).length
      }
      endTime = System.nanoTime()
      log(s"cols: ${cols} " +
      s"bytes: $totalBytes compressed: $totalCompBytes elapsed: " + (endTime - startTime) / 1e6d)
    }
  }
  private val tables = Array("web_returns", "store_sales", "item", "inventory")
  def createViews(): Unit = {
    for (table <- tables) {
      log(s"create table $table")
      val tablePath = s"hdfs://qflock-storage-dc2:9000/tpcds-parquet-100g/${table}.parquet"
      val df0 = spark.read.format("parquet").load(tablePath)
      df0.createOrReplaceTempView(table)
    }
  }
  createViews()
  final def compress(query: String): Unit = {
    //    val df1 = df0.select("cc_call_center_sk")
 //    val df1 = df0.select("cc_call_center_id")
 //    val df1 = df0.select("wr_return_amt")
 //    val df1 = df0.select("wr_returned_date_sk")
 //    val df1 = df0.select("i_item_id")
    //    df1.show(1000, false)
    val df1 = spark.sql(query)
    val values = new Array[Array[Any]](df1.schema.fields.length)
    val bytes = new Array[ByteBuffer](df1.schema.fields.length)
    val compressedBytes = new Array[ByteBuffer](df1.schema.fields.length)
    val compressed = new Array[Array[Byte]](df1.schema.fields.length)
    var startTime = System.nanoTime()
    log(df1.schema.toString)
    val cols = df1.schema.fields.length
    for (a <- Range(0, cols)) {
      // log(s"get row $a " + df1.schema.fields(a).name)
      import spark.implicits._
      values(a) = (df1.schema.fields(a).dataType match {
        case LongType => df1.select(df1.schema.fields(a).name)
          .map(f => f.getLong(0))
        case IntegerType => df1.select(df1.schema.fields(a).name)
          .map(f => f.getInt(0))
        case FloatType => df1.select(df1.schema.fields(a).name)
          .map(f => f.getFloat(0))
        case DoubleType => df1.select(df1.schema.fields(a).name)
          .map(f => f.getDouble(0))
        case StringType => df1.select(df1.schema.fields(a).name)
          .map(f => f.getString(0))
      }).collect.toArray
    }
    val rows = values(0).length
    var endTime = System.nanoTime()
    log(s"cols: ${cols} " +
      s"rows: ${values(0).length} " +
      s"elapsed: " + (endTime - startTime) / 1e6d)
    startTime = System.nanoTime()
    var totalBytes = 0
    for (a <- Range(0, cols)) {
       log(s"get Bytes $a")
      bytes(a) = ByteBuffer.allocate(rows * (df1.schema.fields(a).dataType match {
        case LongType => 8
        case DoubleType => 8
        case StringType => 120 }))
      compressedBytes(a) = ByteBuffer.allocate(rows * (df1.schema.fields(a).dataType match {
        case LongType => 8
        case DoubleType => 8
        case StringType => 120 }))
      values(a).foreach(x => df1.schema.fields(a).dataType match {
        case LongType => bytes(a).putLong(x.asInstanceOf[Long])
        case DoubleType => bytes(a).putDouble(x.asInstanceOf[Double])
        case StringType => bytes(a).put(x.asInstanceOf[String].getBytes)
      })
      totalBytes += bytes(a).position()
      log(s"$a) bytes: ${bytes(a).position()} " + values(a)(0))
    }
    endTime = System.nanoTime()
    log(s"cols: ${cols} " +
      s"totalBytes: $totalBytes elapsed: " + (endTime - startTime) / 1e6d)
    if (true) {
      startTime = System.nanoTime()
      totalBytes = 0
      var totalCompBytes: Long = 0
      for (a <- Range(0, cols)) {
        val compressedByteCount =
          Zstd.compressByteArray(compressedBytes(a).array(), 0, compressedBytes(a).array.length,
                                 bytes(a).array(), 0, bytes(a).position(), 3)
        totalBytes += bytes(a).position()
        totalCompBytes += compressedByteCount
      }
      endTime = System.nanoTime()
      log(s"query $query")
      log(s"cols: ${cols} " +
      s"bytes: $totalBytes compressed: $totalCompBytes elapsed: " + (endTime - startTime) / 1e6d)
    }
  }
  def timeCompress(query: String): Unit = {
    val startTime = System.nanoTime
    compress(query)
    val duration = (System.nanoTime - startTime) / 1e9d
    log(s"duration $duration")
  }
  def readWriteSpark(path: String): Unit = {
 //    log("Starting readWriteTest")
    var startTime = System.nanoTime()
    val df1 = spark.read.option("type", "parquet").load(path)
    df1.write.mode("overwrite")
       .format("parquet").save("/qflock/spark/spark_rd/output")
    val endTime = System.nanoTime()
    log(s"spark cols: ${df1.schema.fields.length} " +
      s"elapsed: " + (endTime - startTime) / 1e6d)
 //    log("Done readWriteTest")
  }
  def readWriteJdbc(path: String): Unit = {
 //    log("Starting readWriteJdbc")
    var startTime = System.nanoTime()
    val df1 = spark.read.format("parquet").load(path)
 //    df1.write.format("parquet").save("logs/output")
    df1.write.format("qflockJdbc")
       .mode("overwrite")
       .option("tempdir", "/qflock/spark/spark_rd")
       .save("logs/output")
    val endTime = System.nanoTime()
    log(s"Jdbc cols: ${df1.schema.fields.length} " +
      s"elapsed: " + (endTime - startTime) / 1e6d)
 //    log("Done readWriteJdbc")
  }
  def recompress(file: String): Unit = {
    val byteArray = Files.readAllBytes(Paths.get(s"/qflock/jdbc/server/${file}_binary_data.bin"))
    val compressedBytes = ByteBuffer.allocate(byteArray.length)
    val compressedByteCount =
          Zstd.compressByteArray(compressedBytes.array(), 0, compressedBytes.array.length,
            byteArray, 0, byteArray.length, 1)
    val ratio = byteArray.length / compressedByteCount
    log(s"bytes ${byteArray.length} compressed ${compressedByteCount} ratio ${ratio} ")
  }
  final def main(args: scala.Array[String]): Unit = {
    val query = if (args.length > 0) {
      args(0)
    } else {
      ""
    }
    log(s"query is: $query")
    val testName = if (args.length > 1) {
      args(1)
    } else ""
    log(s"test is: $testName")
    testName match {
//      case "jdbc" =>
//        for (i <- Range(0, loops)) {
//          readWriteJdbc(path)
//        }
//      case "spark" =>
//        for (i <- Range(0, loops)) {
//          readWriteSpark(path)
//        }
      case "compress" =>
        log(s"query is $query")
        compress(query)
      case "recompress" =>
        log("recompress")
        recompress(query)
    }
//    test(path)
  }
}
object QflockTesta {

  final def main(args: scala.Array[String]): Unit = {
    val query = if (args.length > 0) {
      args(0)
    } else {
      ""
    }
    //    log(s"query is: $query")
    val testName = if (args.length > 1) {
      args(1)
    } else ""
    //    log(s"test is: $testName")
  }
}

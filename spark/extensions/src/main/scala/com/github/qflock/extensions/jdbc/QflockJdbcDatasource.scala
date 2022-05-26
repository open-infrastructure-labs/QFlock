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
package com.github.qflock.extensions.jdbc

import java.net.URI
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.slf4j.LoggerFactory

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap


/** Creates a data source object for Spark that
 *  supports pushdown of predicates such as Filter, Project and Aggregate.
 *
 */
class QflockJdbcDatasource extends TableProvider
  with SessionConfigSupport with DataSourceRegister {

  private val logger = LoggerFactory.getLogger(getClass)
  override def toString: String = s"GenericPushdownDataSource()"
  override def supportsExternalMetadata(): Boolean = true

  /** Fetches a list of FileStatus objects for this directory, or
   *  if the filePath is a file, just a list containing the file's FileStatus.
   *  @param filePath the file or directory path.
   *  @return Seq[FileStatus]
   */
  def getFileStatusList(filePath: String): Seq[FileStatus] = {
    val conf = new Configuration()
    val server = filePath.split("/")(2)
    val endpoint = "hdfs://" + server + {if (filePath.contains(":9000")) "" else ":9000"}
    val fs: FileSystem = FileSystem.get(URI.create(endpoint), conf)
    val fileStatusArray = {
      var statusArray = Array[FileStatus]()
      val fileToRead = new Path(filePath)
      val status = fs.listStatus(fileToRead)
      for (item <- status) {
        if (item.isFile && item.getPath.getName.contains(".parquet")) {
          val fileStatus = fs.getFileStatus(item.getPath)
          statusArray = statusArray ++ Array(fileStatus)
        }
      }
      statusArray.toSeq
    }
    fileStatusArray
  }
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    if (options.get("format") == "parquet") {
      QflockJdbcDatasource.getSchema(options)
    } else {
      /* Other types like CSV require a user-supplied schema */
      throw new IllegalArgumentException("requires a user-supplied schema")
    }
  }
  override def getTable(schema: StructType,
                        transforms: Array[Transform],
                        options: util.Map[String, String]): Table = {
    logger.trace("getTable: Options " + options)
    new QflockJdbcBatchTable(schema, options)
  }

  override def keyPrefix(): String = {
    "qflockJdbc"
  }
  override def shortName(): String = "qflockJdbc"
}

object QflockJdbcDatasource {
  var initialized = false

  def getSchema(options: util.Map[String, String]): StructType = {
    if (options.getOrDefault("schema", "") != "") {
      StructType(options.get("schema").split(",").map(x => {
        val items = x.split(":")
        val dataType = items(1) match {
          case "string" => StringType
          case "integer" => IntegerType
          case "double" => DoubleType
          case "long" => LongType
        }
        val nullable = items(2) match {
          case "false" => false
          case _ => true
        }
        StructField(items(0), dataType, nullable)
      }))
    } else {
      new StructType()
    }
  }
}

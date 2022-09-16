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
package com.github.qflock.datasource

import java.util

import com.github.qflock.extensions.jdbc.QflockJdbcDatasource
import org.slf4j.LoggerFactory

import org.apache.spark.sql.connector.catalog.{SessionConfigSupport, Table, TableProvider}
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** Creates a data source object use for handling
 *  queries on the remote spark server.
 *  The main purpose of this data source is to be able to handle
 *  a subset of row groups for a parquet file.
 *  The QflockTableDescriptor gives the client an api to specify
 *  the range of row groups.
 *  The tableName datasource option specifies the table to partition on row groups.
 *  The requestId specifies the table descriptor's requestId which contains the
 *  range of row groups to operate on.
 *  See the QflockScan for the code where we actually create the partitions.
 */
class QflockDatasource extends TableProvider
  with SessionConfigSupport with DataSourceRegister {

  private val logger = LoggerFactory.getLogger(getClass)
  override def toString: String = s"QflockDs()"
  override def supportsExternalMetadata(): Boolean = true

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
    val path = options.get("path")
    logger.trace("getTable: Options " + options)
    new QflockBatchTable(schema, options, path)
  }

  override def keyPrefix(): String = {
    "qflockDs"
  }
  override def shortName(): String = "qflockDs"
}


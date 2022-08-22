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

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, SupportsTruncate, WriteBuilder}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap


/** Creates a Table object that supports pushdown predicates
 *   such as Filter, Project, and Aggregate.
 *
 * @param schema the StructType format of this table
 * @param options the parameters for creating the table
 *                "endpoint" is the server name,
 *                "accessKey" and "secretKey" are the credentials for above server.
 *                 "path" is the full path to the s3 file.
 */
class QflockJdbcBatchTable(schema: StructType,
                           options: util.Map[String, String])
  extends Table with SupportsRead with SupportsWrite {

  override def name(): String = this.getClass.toString

  override def schema(): StructType = schema

  override def capabilities(): util.Set[TableCapability] = {
    Set(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE, TableCapability.TRUNCATE).asJava
  }

  override def newScanBuilder(params: CaseInsensitiveStringMap): ScanBuilder =
    new QflockJdbcScanBuilder(schema, options)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new QflockJdbcWriteBuilder(info)
}


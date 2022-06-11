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

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType


/** Creates a builder for scan objects.
 *
 * @param schema the format of the columns
 * @param options the options to the datasource
 */
class QflockJdbcScanBuilder(schema: StructType,
                            options: util.Map[String, String])
  extends ScanBuilder {

  /** Returns a scan object for this particular query.
   *   Currently we only support S3 and Hdfs.
   *
   * @return the scan object either a S3Scan or HdfsScan
   */
  override def build(): Scan = {
    /* Make the map modifiable.
     * The objects below can override defaults.
     */
    val opt: util.Map[String, String] = new util.HashMap[String, String](options)
    if (!options.get("path").contains("hdfs")) {
      throw new Exception(s"endpoint ${options.get("endpoint")} is unexpected")
    }
    QflockJdbcScan(schema, opt, None)
  }
}


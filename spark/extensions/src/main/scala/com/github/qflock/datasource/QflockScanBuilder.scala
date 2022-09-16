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

import org.slf4j.LoggerFactory

import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._


/** Creates a builder for scan objects.
 *
 * @param schema the format of the columns
 * @param options the options (see PushdownBatchTable for full list.)
 */
class QflockScanBuilder(schema: StructType,
                        options: util.Map[String, String],
                        path: String)
  extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns {

  private val logger = LoggerFactory.getLogger(getClass)
  var pushedFilter: Array[Filter] = new Array[Filter](0)
  private var prunedSchema: StructType = schema

  /** Returns a scan object for this particular query.
   *   Currently we only support Hdfs.
   *
   * @return the scan object a HdfsScan
   */
  override def build(): Scan = {
    /* Make the map modifiable.
     * The objects below can override defaults.
     */
    val opt: util.Map[String, String] = new util.HashMap[String, String](options)

    if (!path.contains("hdfs")) {
      throw new Exception(s"endpoint ${options.get("endpoint")} is unexpected")
    }
    opt.put("path", path)
    new QflockScan(schema, opt, pushedFilter, prunedSchema)
  }
  /** Pushes down the list of columns specified by requiredSchema
   *
   * @param requiredSchema the list of columns we should use, and prune others.
   */
  override def pruneColumns(requiredSchema: StructType): Unit = {
    prunedSchema = requiredSchema
//    logger.info("pruneColumns " + requiredSchema.toString)
  }

  override def pushedFilters: Array[Filter] = {
    logger.info("pushedFilters" + pushedFilter.toList)
    pushedFilter
  }

  /** Pushes down a list of filters.  We assume the filters
   *  are "and" separated. e.g. filter1 and filter2 and filter3, etc.
   *
   * @param filters the list of filters to push down
   * @return list of filters to be re-evaluated upon completion of query.
   */
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    logger.trace("pushFilters" + filters.toList)
    pushedFilter = filters
    // In this case we know that we need to re-evaluate the filters
    // since the pushdown cannot guarantee application of the filter.
    filters
  }
}

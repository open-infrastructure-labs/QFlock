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
/* Some portions of this code, most notably getColumnSchema,
 * were inspired by this patch by Huaxin Gao:
 *   https://github.com/apache/spark/pull/29695
 */
package com.github.qflock.datasource.common

import java.sql.{Date, Timestamp}
import java.util
import java.util.{HashMap, Locale, StringTokenizer}

import scala.collection.mutable.ArrayBuilder

import org.slf4j.LoggerFactory

import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, Aggregation => ExprAgg, Count, CountStar, Max, Min, Sum}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._


/** Provides pushdown capabilities aimed at
 *  generating information needed for pushdown
 *  from the inputs that Spark provides.
 */
class Pushdown(val schema: StructType, val prunedSchema: StructType,
               val filters: Seq[Filter],
               val aggregation: Option[ExprAgg],
               val options: util.Map[String, String]) extends Serializable {

  protected val logger = LoggerFactory.getLogger(getClass)

  protected var supportsIsNull = !options.containsKey("DisableSupportsIsNull")

  def isPushdownNeeded: Boolean = {
    /* Determines if we should send the pushdown to ndp.
     * If any of the pushdowns are in use (project, filter, aggregate),
     * then we will consider that pushdown is needed.
     */
    ((prunedSchema.length != schema.length) ||
     (filters.length > 0))
  }
  /**
   * `columns`, but as a String suitable for injection into a SQL query.
   */
  def getColumnSchema(): (String, StructType) = {
    val sb = new StringBuilder()
    var updatedSchema: StructType = new StructType()
    val cols = prunedSchema.fields.map(x => {
      getColString(x.name)
    }).toArray
    updatedSchema = prunedSchema
    cols.foreach(x => sb.append(",").append(x))
    (if (sb.length == 0) "" else sb.substring(1),
      if (sb.length == 0) prunedSchema else updatedSchema)
  }

  /** returns the representation of the column name according to the
   *  current option set.
   *  @param attr - Attribute name
   *  @return String - representation of the column name.
   */
  def getColString(attr: String): String = {
    val colString = s"${attr}"
    colString
  }

  val (readColumns: String,
       readSchema: StructType) = {
    var (columns, updatedSchema) =
      getColumnSchema()
    (columns,
     if (updatedSchema.names.isEmpty) schema else updatedSchema)
  }
}


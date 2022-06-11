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
package com.github.qflock.extensions.rules

import java.util

import scala.collection.convert.ImplicitConversions.`map AsScala`

import org.apache.hadoop.hive.metastore.api.Table
import org.slf4j.{Logger, LoggerFactory}

import org.apache.spark.Partition
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, CatalogTable}
import org.apache.spark.sql.catalyst.expressions.{AttributeMap, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Project, Statistics}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.BasicStatsPlanVisitor
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.extension.ExtHiveUtils
import org.apache.spark.sql.sources.BaseRelation

case class QflockStatsParameters(val project: Seq[NamedExpression],
                                 val filterCondition: Option[Expression],
                                 val relationArgs: QflockRelationArgs,
                                 val attrReferences: Seq[AttributeReference],
                                 val filterReferences: Seq[AttributeReference],
                                 val opt: util.HashMap[String, String],
                                 val references: Seq[AttributeReference]) {

}

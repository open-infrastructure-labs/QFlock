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

import java.io.FileWriter
import java.util.HashMap

import scala.collection.mutable
import scala.util.{Either, Left => EitherLeft, Right => EitherRight}

import org.json._
import org.slf4j.LoggerFactory
import sys.process._

import org.apache.spark.SparkFiles
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer.ext.CostBasedJoinReorderExt
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter => LogicalFilter}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.types._

case class FederationRule(spark: SparkSession) extends Rule[LogicalPlan] {
  protected val logger = LoggerFactory.getLogger(getClass)
  override def apply(plan: LogicalPlan): LogicalPlan = {
    logger.info(s"FederationRule LogicalPlan $plan")
    val fw = new FileWriter("./rules.txt", false)
    try {
      fw.write(plan.toString() + "\n")
    }
    finally fw.close()
    plan
  }
}
class FederationExtensions extends (SparkSessionExtensions => Unit) {
  def apply(e: SparkSessionExtensions): Unit = {
    e.injectOptimizerRule(CostBasedJoinReorderExt)
    e.injectOptimizerRule(FederationRule)
  }
}

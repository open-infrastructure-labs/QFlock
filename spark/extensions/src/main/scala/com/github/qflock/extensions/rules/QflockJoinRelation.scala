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
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.BasicStatsPlanVisitor
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.extension.ExtHiveUtils
import org.apache.spark.sql.sources.BaseRelation


class QflockJoinRelation(override val relation: BaseRelation,
                         override val output: Seq[AttributeReference],
                         override val catalogTable: Option[CatalogTable],
                         override val isStreaming: Boolean)
                        (val rowCount: Option[BigInt],
                         val relationArgs: QflockRelationArgs)
  extends LogicalRelation(relation, output, relationArgs.catalogTable, isStreaming) {
  protected val logger: Logger = LoggerFactory.getLogger(getClass)
  def getArgs: Option[(BaseRelation, Seq[AttributeReference], Option[CatalogTable], Boolean)] =
  Some(relation, output, catalogTable, isStreaming)
  override protected final def otherCopyArgs: Seq[AnyRef] = {
    rowCount :: Nil
  }
  private val statsParam = relationArgs.statsParam.get.asInstanceOf[QflockJoinStatsParameters]
  private val tables = statsParam.catalogTables.get.map(t =>
    ExtHiveUtils.getTable(t.identifier.database.getOrElse(""),
                          t.identifier.table))
  def getTableForCol(colName: String): CatalogTable =
    statsParam.catalogTables.get.filter(t =>
      t.stats.get.colStats.contains(colName)).head

  private val colStats =
    output.map(a => a.name -> getTableForCol(a.name).stats.get.colStats(a.name)).toMap

  def getTableForColumn(colName: String): Table = {
    tables.filter(t =>
      t.getParameters.containsKey(QflockJoinRelation.getColSizeParamName(colName))).head
  }
  val (colsBytes: Double, colsNames: String) = {
    var bytes: Double = 0
    var cols = ""
    for (x <- output) {
      val size = QflockJoinRelation.getColSize(getTableForColumn(x.name), x.name)
      bytes += size
      cols += s"${x.name}: $size "
    }
    (bytes, cols)
  }
  val sizeInBytes: Int = {
    val rows = rowCount.getOrElse(BigInt(1)).longValue()
    output.foldLeft(0)((x, y) => {
      x +
      QflockLogicalRelation.getColSize(getTableForColumn(y.name), y.name) * rows}.asInstanceOf[Int])
//     stats.sizeInBytes.longValue().asInstanceOf[Int]
  }
  def toPlanStats: Statistics = {
    val left = statsParam.catalogTables.get.head.stats.get
    val right = statsParam.catalogTables.get(1).stats.get
    if (rowCount.isDefined) {
      val planStats1 = output.flatMap(a =>
        left.colStats.get(a.name).map(a -> _.toPlanStat(a.name, a.dataType)))
      val planStats2 = output.flatMap(a =>
                       right.colStats.get(a.name).map(a -> _.toPlanStat(a.name, a.dataType)))
      val attrStats = AttributeMap(planStats1 ++ planStats2)
      // Estimate size as number of rows * row size.
      // val size = EstimationUtils.getOutputSize(planOutput, rowCount.get, attrStats)
      val size = BigDecimal.valueOf(colsBytes * rowCount.get.toDouble).toBigInt()
      Statistics(sizeInBytes = size, rowCount = rowCount, attributeStats = attrStats)
    } else {
      // When plan statistics are disabled or the table doesn't have other statistics,
      // we apply the size-only estimation strategy and only propagate sizeInBytes in statistics.
      Statistics(sizeInBytes = left.sizeInBytes)
    }
  }
  override def computeStats(): Statistics = {
    toPlanStats
//    catalogTable
//      .flatMap(_.stats.map(x =>
//        new CatalogStatistics(x.sizeInBytes,
//          rowCount,
//          x.colStats)
//          .toPlanStats(output, true)))
//      .getOrElse(Statistics(sizeInBytes = relation.sizeInBytes))
  }
  override def simpleString(maxFields: Int): String =
    s"QflockLogicalRelation " +
      s"size:$sizeInBytes rows:${rowCount.get} colsSize:$colsBytes " +
      s"cols:$colsNames " +
      s"${catalogTable.map(_.identifier.unquotedString).getOrElse("")} " +
      s"[${truncatedString(output, ",", maxFields)}] $relation"
}


object QflockJoinRelation {
  def unapply(relation: QflockLogicalRelation):
  Option[(BaseRelation, Seq[AttributeReference], Option[CatalogTable], Boolean)] = {
    relation.getArgs
  }
  def getColSizeParamName(colName: String): String =
    s"spark.qflock.statistics.colStats.$colName.bytes_per_row"
  def getColSize(table: Table, colName: String): Double = {
    table.getParameters.get(getColSizeParamName(colName)).toDouble
  }
  def apply(relationArgs: QflockRelationArgs,
            joinPlan: LogicalPlan,
            opt: util.HashMap[String, String],
            references: Seq[AttributeReference],
            spark: SparkSession): QflockJoinRelation = {
    val scalaOpts = scala.collection.immutable.HashMap(opt.toSeq: _*)
    val qflockRelation = QflockRelation(references.toStructType,
      Array.empty[Partition], scalaOpts)(spark)
    val planStats = BasicStatsPlanVisitor.visit(joinPlan)
    val scanRelation = new QflockJoinRelation(qflockRelation.asInstanceOf[BaseRelation],
                                              references, relationArgs.catalogTable,
                                   false)(planStats.rowCount, relationArgs)
    scanRelation
  }
}


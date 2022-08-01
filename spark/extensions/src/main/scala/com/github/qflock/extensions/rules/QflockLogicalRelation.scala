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


class QflockLogicalRelation(override val relation: BaseRelation,
                            override val output: Seq[AttributeReference],
                            override val catalogTable: Option[CatalogTable],
                            override val isStreaming: Boolean)
                           (val rowCount: Option[BigInt],
                            val previousSizeInBytes: BigInt = BigInt(0),
                            val previousRows: BigInt = BigInt(0),
                            val estimate: Option[Boolean] = Some(false))
  extends LogicalRelation(relation, output, catalogTable, isStreaming) {
  protected val logger: Logger = LoggerFactory.getLogger(getClass)
  def getArgs: Option[(BaseRelation, Seq[AttributeReference], Option[CatalogTable], Boolean)] =
  Some(relation, output, catalogTable, isStreaming)
  var tableName: String = catalogTable.get.identifier.table
  var dbName: String = catalogTable.get.identifier.database.getOrElse("")
  def isEstimate: Boolean = estimate.getOrElse(false)
  override protected final def otherCopyArgs: Seq[AnyRef] = {
    rowCount :: previousSizeInBytes :: previousRows :: estimate :: Nil
  }
  private val table = ExtHiveUtils.getTable(dbName, tableName)
  val (colsBytes: Double, colsNames: String) = {
    var bytes: Double = 0
    var cols = ""
    for (x <- output) {
      val size = QflockLogicalRelation.getColSize(table, x.name)
      bytes += size
      cols += s"${x.name}: $size "
    }
    (bytes, cols)
  }
  val sizeInBytes: Int = {
    val rows = rowCount.getOrElse(BigInt(1)).longValue()
    output.foldLeft(0)((x, y) => x +
          (QflockLogicalRelation.getColSize(table, y.name) * rows).asInstanceOf[Int])
//     stats.sizeInBytes.longValue().asInstanceOf[Int]
  }
  def toPlanStats(catStat: CatalogStatistics): Statistics = {
    if (rowCount.isDefined) {
      val attrStats = AttributeMap(output
        .flatMap(a => catStat.colStats.get(a.name).map(a -> _.toPlanStat(a.name, a.dataType))))
      // Estimate size as number of rows * row size.
      // val size = EstimationUtils.getOutputSize(planOutput, rowCount.get, attrStats)
      val size = BigDecimal.valueOf(colsBytes * rowCount.get.toDouble).toBigInt()
      Statistics(sizeInBytes = size, rowCount = rowCount, attributeStats = attrStats)
    } else {
      // When plan statistics are disabled or the table doesn't have other statistics,
      // we apply the size-only estimation strategy and only propagate sizeInBytes in statistics.
      Statistics(sizeInBytes = catStat.sizeInBytes)
    }
  }
  override def computeStats(): Statistics = {
    catalogTable
      .flatMap(_.stats.map(x =>
        toPlanStats(CatalogStatistics(x.sizeInBytes,
          rowCount,
          x.colStats))))
      .getOrElse(Statistics(sizeInBytes = relation.sizeInBytes))
//    catalogTable
//      .flatMap(_.stats.map(x =>
//        new CatalogStatistics(x.sizeInBytes,
//          rowCount,
//          x.colStats)
//          .toPlanStats(output, true)))
//      .getOrElse(Statistics(sizeInBytes = relation.sizeInBytes))
  }
  override def simpleString(maxFields: Int): String =
    s"QflockLogicalRelation prevSize:$previousSizeInBytes prevRows:$previousRows " +
      s"size:$sizeInBytes rows:${rowCount.get} colsSize:$colsBytes " +
      s"cols:$colsNames " +
      s"${catalogTable.map(_.identifier.unquotedString).getOrElse("")} " +
      s"[${truncatedString(output, ",", maxFields)}] $relation"
}

object QflockLogicalRelation {
  def unapply(relation: QflockLogicalRelation):
  Option[(BaseRelation, Seq[AttributeReference], Option[CatalogTable], Boolean)] = {
    relation.getArgs
  }
  def getColSize(table: Table, colName: String): Double = {
    table.getParameters.get(s"spark.qflock.statistics.colStats.$colName.bytes_per_row").toDouble
  }
  def apply(project: Seq[NamedExpression],
            filterCondition: Option[Expression],
            relationArgs: QflockRelationArgs,
            attrReferences: Seq[AttributeReference],
            filterReferences: Seq[AttributeReference],
            opt: util.HashMap[String, String],
            references: Seq[AttributeReference],
            spark: SparkSession): QflockLogicalRelation = {
    val scalaOpts = scala.collection.immutable.HashMap(opt.toSeq: _*)
    val qflockRelation = QflockRelation(references.toStructType,
      Array.empty[Partition], scalaOpts)(spark)
    val planStats = {
      /* We need to visit the plan in order to generate the stats, even when
       * stats is disabled.  In this case we will generate a plan with our
       * project and filter, and then visit it to gather the Statistics.
       */
      val qLogRel = new QflockLogicalRelationWithStats(
        relationArgs.relation.asInstanceOf[BaseRelation],
        relationArgs.output,
        relationArgs.catalogTable, false)()
      val filterPlan = filterCondition.map(QflockFilter(_, qLogRel)).getOrElse(qLogRel)
      val projPlan = Project(project, filterPlan)
      BasicStatsPlanVisitor.visit(projPlan)
    }
    val (relationSizeInBytes, relationRows) = {
      // Just add the size required by the filter to the size required by the project.
      val relationRows = if (filterReferences.nonEmpty) {
        val filterRelationV = new QflockLogicalRelationWithStats(
          qflockRelation.asInstanceOf[BaseRelation],
          filterReferences,
          relationArgs.catalogTable, false)()
        val relationRowCount = BasicStatsPlanVisitor.visit(filterRelationV).rowCount
        relationRowCount.get
      } else {
        // No filter needed, let's just not consider the size of the filter.
        val relationForVisit = new QflockLogicalRelationWithStats(
          qflockRelation.asInstanceOf[BaseRelation],
          attrReferences,
          relationArgs.catalogTable, false)()
        val relationRowCount = BasicStatsPlanVisitor.visit(relationForVisit).rowCount
        relationRowCount.get
      }
      val refs = QflockUtils.distinctReferences(attrReferences, filterReferences)
      // This is the size required by the project,
      // but less any columns that are also needed by the filter since
      // those columns are already included in the filter.
      val projectRelationBytes = {
        val qLogRel = new QflockLogicalRelation(
          qflockRelation.asInstanceOf[BaseRelation],
          refs,
          relationArgs.catalogTable, false) (Some(relationRows))
        qLogRel.sizeInBytes
      }
      (projectRelationBytes, relationRows)
    }
    val scanRelation = new QflockLogicalRelation(qflockRelation.asInstanceOf[BaseRelation],
      references, relationArgs.catalogTable,
      false)(planStats.rowCount,
      relationSizeInBytes, relationRows)
    scanRelation
  }
}

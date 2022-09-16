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

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{AttributeMap, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Statistics}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.connector.read.{Scan, Statistics => V2Statistics, SupportsReportStatistics}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}

/**
 * This overrides the DataSourceV2ScanRelation
 * in order that we can provide stats attributes for more fine grained
 * statistics estimations.
 *
 * @param relation a [[DataSourceV2Relation]]
 * @param scan a DSv2 [[Scan]]
 * @param output the output attributes of this relation
 */
class QflockDataSourceV2ScanRelation(
                                     override val relation: DataSourceV2Relation,
                                     override val scan: Scan,
                                     override val output: Seq[AttributeReference],
                                     keyGroupedPartitioning: Option[Seq[Expression]] = None,
                                     catalogTable: CatalogTable)
  extends DataSourceV2ScanRelation(relation, scan, output, keyGroupedPartitioning) {
  def getArgs: Option[(DataSourceV2Relation, Scan, Seq[AttributeReference],
    Option[Seq[Expression]], CatalogTable)] =
    Some(relation, scan, output, keyGroupedPartitioning, catalogTable)
  override protected final def otherCopyArgs: Seq[AnyRef] = {
    catalogTable :: Nil
  }

  override def name: String = relation.table.name()

  override def simpleString(maxFields: Int): String = {
    s"RelationV2${truncatedString(output, "[", ", ", "]", maxFields)} $name"
  }

  override def computeStats(): Statistics = {
    scan match {
      case r: SupportsReportStatistics =>
        val statistics = r.estimateStatistics()
        val catalogStats = catalogTable.stats.get
        val attributeStats = AttributeMap(output
        .flatMap(a => catalogStats.colStats.get(a.name).map(a -> _.toPlanStat(a.name, a.dataType))))
        QflockDataSourceV2ScanRelation.transformV2Stats(statistics,
                                                        None,
                                                        conf.defaultSizeInBytes,
                                                        attributeStats)
      case _ =>
        Statistics(sizeInBytes = conf.defaultSizeInBytes)
    }
  }
}

object QflockDataSourceV2ScanRelation {
  /**
   * This is used to transform data source v2 statistics to logical.Statistics.
   */
  def transformV2Stats(v2Statistics: V2Statistics,
                       defaultRowCount: Option[BigInt],
                       defaultSizeInBytes: Long,
                       attributeStats: AttributeMap[ColumnStat] = AttributeMap(Nil)): Statistics = {
    val numRows: Option[BigInt] = if (v2Statistics.numRows().isPresent) {
      Some(v2Statistics.numRows().getAsLong)
    } else {
      defaultRowCount
    }
    Statistics(sizeInBytes = v2Statistics.sizeInBytes().orElse(defaultSizeInBytes),
               rowCount = numRows,
               attributeStats = attributeStats)
  }
}

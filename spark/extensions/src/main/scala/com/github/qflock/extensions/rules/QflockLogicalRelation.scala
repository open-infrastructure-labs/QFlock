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

import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, CatalogTable}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.BaseRelation

class QflockLogicalRelation(override val relation: BaseRelation,
                            override val output: Seq[AttributeReference],
                            override val catalogTable: Option[CatalogTable],
                            override val isStreaming: Boolean)
                           (val rowCount: Option[BigInt],
                            val previousSizeInBytes: BigInt,
                            val estimate: Option[Boolean] = Some(false))
  extends LogicalRelation(relation, output, catalogTable, isStreaming) {
  def getArgs: Option[(BaseRelation, Seq[AttributeReference], Option[CatalogTable], Boolean)] =
  Some(relation, output, catalogTable, isStreaming)

  def isEstimate: Boolean = estimate.getOrElse(false)
  override protected final def otherCopyArgs: Seq[AnyRef] = {
    rowCount :: previousSizeInBytes :: estimate :: Nil
  }
  private val sizeInBytes = stats.sizeInBytes.longValue()
  private val previousBytes = previousSizeInBytes.longValue()
  private def savingsInBytes = {
    if (sizeInBytes < previousBytes) {
      previousBytes - sizeInBytes
    } else {
      0L
    }
  }
  override def computeStats(): Statistics = {
    catalogTable
      .flatMap(_.stats.map(x =>
        new CatalogStatistics(x.sizeInBytes,
                              rowCount,
                              x.colStats)
        .toPlanStats(output, true)))
      .getOrElse(Statistics(sizeInBytes = relation.sizeInBytes))
  }
  override def simpleString(maxFields: Int): String =
    s"QflockLogicalRelation prevSize:${previousSizeInBytes} " +
      s"size:${sizeInBytes} " +
      s" ${catalogTable.map(_.identifier.unquotedString).getOrElse("")}" +
      s"[${truncatedString(output, ",", maxFields)}] $relation"
}

object QflockLogicalRelation {
  def unapply(relation: QflockLogicalRelation):
  Option[(BaseRelation, Seq[AttributeReference], Option[CatalogTable], Boolean)] = {
    relation.getArgs
  }
}

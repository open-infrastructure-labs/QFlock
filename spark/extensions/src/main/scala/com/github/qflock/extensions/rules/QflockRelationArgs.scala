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

import scala.collection.JavaConverters._

import com.github.qflock.extensions.jdbc.QflockJdbcScan
import com.github.qflock.extensions.remote.QflockRemoteScan
import org.apache.hadoop.hive.metastore.api.Table

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.hive.extension.ExtHiveUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** Contains the details extracted from a relation and its children.
 *
 * @param relation typically the base relation
 * @param scan the scan or base relation
 * @param output output columns
 * @param dataSchema full schema
 * @param readSchema read column schema
 * @param options options for data source
 * @param statsParam statistics parameters for additional pushdown.
 * @param catalogTable catalog table (optional)
 */
case class QflockRelationArgs(relation: Any, scan: Any, output: Seq[AttributeReference],
                              dataSchema: StructType, readSchema: StructType,
                              options: CaseInsensitiveStringMap,
                              statsParam: Option[Any],
                              catalogTable: Option[CatalogTable]) {
  def getTable: Table = {
    val catTable = catalogTable.get
    val tableName = catTable.identifier.table
    val dbName = catTable.identifier.database.getOrElse("")
    ExtHiveUtils.getTable(dbName, tableName)
  }
}

object QflockRelationArgs {
  /** Parses the input child node and extracts all the relevant fields
   *  we might need later.
   *
   * @param child Child node to parse
   * @return Option[QflockRelationArgs]
   */
  def apply(child: Any): Option[QflockRelationArgs] = {
    val (relation, scan, output, catalogTable) = child match {
      case DataSourceV2ScanRelation(relation, scan, output, _) =>
        val catalogTable = scan match {
          case QflockJdbcScan(_, _, statsParam, _) =>
            if (statsParam.isDefined) {
              statsParam.get match {
                case s: QflockStatsParameters =>
                  s.relationArgs.catalogTable
                case s: QflockJoinStatsParameters =>
                  s.relationArgs.catalogTable
              }
            } else {
              None
            }
          case QflockRemoteScan(_, _, statsParam, _) =>
            if (statsParam.isDefined) {
              statsParam.get match {
                case s: QflockStatsParameters =>
                  s.relationArgs.catalogTable
                case s: QflockJoinStatsParameters =>
                  s.relationArgs.catalogTable
              }
            } else {
              None
            }
        }
        (relation, scan, output, catalogTable)
      case LogicalRelation(relation, output, table, _) =>
        (relation, relation, output, table)
    }
    val (dataSchema, readSchema, options, statsParam) = scan match {
      case ParquetScan(_, _, _, dataSchema, readSchema, _, _, opts, _, _, _) =>
        (dataSchema, readSchema, opts, None)
      case HadoopFsRelation(_, _, dataSchema, _, _, opts) =>
        (dataSchema, dataSchema, new CaseInsensitiveStringMap(opts.asJava), None)
      case QflockRelation(schema, _, opts) =>
        (schema, schema, new CaseInsensitiveStringMap(opts.asJava), None)
      case QflockJdbcScan(schema, opts, statsParam, _) =>
        (schema, schema, new CaseInsensitiveStringMap(opts), statsParam)
      case QflockRemoteScan(schema, opts, statsParam, _) =>
        (schema, schema, new CaseInsensitiveStringMap(opts), statsParam)
    }
    Some(new QflockRelationArgs(relation, scan, output, dataSchema,
                                readSchema, options, statsParam, catalogTable))
  }
}

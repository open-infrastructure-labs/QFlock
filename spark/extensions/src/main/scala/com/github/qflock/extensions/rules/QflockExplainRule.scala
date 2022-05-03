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

import java.io.FileWriter
import java.util
import java.util.HashMap

import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`map AsScala`
import scala.collection.mutable
import scala.sys.process._
import scala.util.{Either, Left => EitherLeft, Right => EitherRight}

import com.github.qflock.extensions.common.{PushdownJson, PushdownJsonStatus, PushdownSQL, PushdownSqlStatus}
import com.github.qflock.extensions.jdbc.QflockJdbcScan
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.session.SessionState
import org.json._
import org.slf4j.LoggerFactory

import org.apache.spark.{Partition, SparkFiles}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter => LogicalFilter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.BasicStatsPlanVisitor
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.util._


case class QflockExplainRule(spark: SparkSession) extends Rule[LogicalPlan] {
  protected val appId = spark.sparkContext.applicationId
  private def getAttribute(origExpression: Any) : Either[String, AttributeReference] = {
    origExpression match {
      case Alias(child, name) =>
        getAttribute(child)
      case Cast(expression, dataType, timeZoneId, _) =>
        getAttribute(expression)
      case attrib @ AttributeReference(name, dataType, nullable, meta) =>
        EitherRight(attrib)
      case default => EitherLeft("Unknown Attribute: " + default)
    }
  }
  private def getAttributeReferences(project: Seq[NamedExpression]):
  Either[String, Seq[AttributeReference]] = {
    var failed = false
    val attributes = project.flatMap {x =>
      getAttribute(x) match {
        case EitherLeft(l) => logger.info(l)
          failed = true
          Seq[AttributeReference]()
        case EitherRight(r) =>
          Seq(r)
      }
    }
    if (failed) {
      EitherLeft("Failed attribute references.")
    } else {
      EitherRight(attributes)
    }
  }

  def getFilterAttributes(filters: Seq[Expression]): Either[String, Seq[AttributeReference]] = {
    var failed = false
    val attributes = filters.flatMap(f => {
      val attrSeq = getFilterExpressionAttributes(f)
      if (attrSeq.length == 0) {
        failed = true
      }
      attrSeq
    })
    if (failed) {
      EitherLeft("Failed getting filter expr attributes")
    } else {
      EitherRight(attributes)
    }
  }
  def getFilterExpressionAttributes(filter: Expression): Seq[AttributeReference] = {
    filter match {
      case attrib @ AttributeReference(name, dataType, nullable, meta) =>
        Seq(attrib)
      case Cast(expression, dataType, timeZoneId, _) =>
        getFilterExpressionAttributes(expression)
      case Or(left, right) => getFilterExpressionAttributes(left) ++
        getFilterExpressionAttributes(right)
      case And(left, right) => getFilterExpressionAttributes(left) ++
        getFilterExpressionAttributes(right)
      case Not(filter) => getFilterExpressionAttributes(filter)
      case In(attr, list) => getFilterExpressionAttributes(attr)
      case EqualTo(attr, value) => getFilterExpressionAttributes(attr)
      case LessThan(attr, value) => getFilterExpressionAttributes(attr)
      case GreaterThan(attr, value) => getFilterExpressionAttributes(attr)
      case LessThanOrEqual(attr, value) => getFilterExpressionAttributes(attr)
      case GreaterThanOrEqual(attr, value) => getFilterExpressionAttributes(attr)
      case IsNull(attr) => getFilterExpressionAttributes(attr)
      case IsNotNull(attr) => getFilterExpressionAttributes(attr)
      case StartsWith(left, right) => getFilterExpressionAttributes(left)
      case EndsWith(left, right) => getFilterExpressionAttributes(left)
      case Contains(left, right) => getFilterExpressionAttributes(left)
      case other@_ => logger.warn("unknown filter:" + other) ; Seq[AttributeReference]()
    }
  }
  private def needsRule(project: Seq[NamedExpression],
                        filters: Seq[Expression],
                        child: Any): Boolean = {
    child match {
      case DataSourceV2ScanRelation(relation, scan, output) =>
        (!scan.isInstanceOf[QflockJdbcScan])
      case qlr@QflockLogicalRelation(relation, output, table, _) =>
        relation match {
          // If we injected it just for size estimates, allow it to continue.
          case q@QflockRelation(_, _, _) if (qlr.isEstimate) => true
          case q@QflockRelation(_, _, _) => false
          case _ => true
        }
      case LogicalRelation(relation, output, table, _) => true
      case _ => false
    }
  }
  def canHandlePlan(project: Seq[NamedExpression],
                    filters: Seq[Expression],
                    child: Any,
                    alwaysInject: Boolean = true): Boolean = {
    val relationArgsOpt = RelationArgs(child)
    if (relationArgsOpt == None) {
      return false
    }
    val relationArgs = relationArgsOpt.get
    //    if (relationArgs.dataSchema == relationArgs.readSchema) {
    //      logger.warn("Plan not modified. No Project Necessary. " +
    //        relationArgs.options.get("currenttest"))
    //      return false
    //    }
    val attrReferencesEither = getAttributeReferences(project)
    if (attrReferencesEither.isLeft) {
      logger.warn("Plan not modified due to project")
      alwaysInject
    } else {
      val filterReferencesEither = getFilterAttributes(filters)
      if (filterReferencesEither.isLeft) {
        logger.warn("Plan not modified due to filter")
        alwaysInject
      } else if (false && !filters.exists(x => !x.isInstanceOf[IsNotNull])) {
        // We only pushdown if there are some filters that are not (IsNotNull).
        // logger.warn("Plan has no filters ")
        relationArgs.scan match {
            case QflockRelation(schema, parts, opts) => false
            case _ => alwaysInject
        }
      } else {
        true
      }
    }
  }
  private def transformProject(plan: LogicalPlan,
                               project: Seq[NamedExpression],
                               filters: Seq[Expression],
                               child: LogicalPlan)
  : LogicalPlan = {
    val relationArgs = RelationArgs(child).get
    val attrReferencesEither = getAttributeReferences(project)

    val attrReferences = attrReferencesEither match {
      case EitherRight(r) => r
      case EitherLeft(l) => Seq[AttributeReference]()
    }
    val filterReferencesEither = getFilterAttributes(filters)
    val filterReferences = filterReferencesEither match {
      case EitherRight(r) => r
      case EitherLeft(l) => Seq[AttributeReference]()
    }
    val opt = new util.HashMap[String, String](relationArgs.options)
    val path = opt.get("path") // .replaceFirst("hdfs://.*:9000/", "hdfs://dikehdfs:9860/")
    opt.put("path", path)
    opt.put("format", "parquet")
    opt.put("outputFormat", "binary")
    val filtersStatus = PushdownSqlStatus.FullyValid
    var references = {
      filtersStatus match {
        case PushdownSqlStatus.Invalid =>
          (attrReferences ++ filterReferences).distinct
        case PushdownSqlStatus.PartiallyValid =>
          (attrReferences ++ filterReferences).distinct
        case PushdownSqlStatus.FullyValid =>
          attrReferences.distinct
      }
    }
    logger.info(s"Stats ${plan.stats}")
    val filterCondition: Option[Expression] = filters.reduceLeftOption(And)
    val scanRelation = QflockLogicalRelation.apply(project, filterCondition, relationArgs,
                                  attrReferences, filterReferences, opt, references, spark)
    val withFilter = {
      if (filtersStatus == PushdownSqlStatus.FullyValid) {
        /* Clip the filter from the DAG, since we are going to
         * push down the entire filter to NDP.
         */
        scanRelation
      } else {
        filterCondition.map(LogicalFilter(_, scanRelation)).getOrElse(scanRelation)
      }
    }
    if (withFilter.output != project || filters.length == 0) {
      if (project != scanRelation.output) {
        Project(project, withFilter)
      } else {
        scanRelation
      }
    } else {
      withFilter
    }
  }
  def getCatalogProperties(catalog: CatalogTable): Map[String, String] = {
    logger.info(catalog.toString())
    catalog match {
      case CatalogTable(_, _, _, _, _, _, _, _, _, _, _, prop, _, _, _, _, _, _, _, _) =>
      prop
    }
  }
  private def getScanRelation(project: Seq[NamedExpression], filters: Seq[Expression],
                              child: LogicalPlan, relationArgs: RelationArgs,
                              attrReferences: Seq[AttributeReference],
                              filterReferences: Seq[AttributeReference],
                              opt: util.HashMap[String, String],
                              references: Seq[AttributeReference]) = {
    val scalaOpts = scala.collection.immutable.HashMap(opt.toSeq: _*)
    val needsPushdown = canHandlePlan(project, filters, child, alwaysInject = false)
    val qflockRelation = new QflockRelation(references.toStructType,
      Array.empty[Partition], scalaOpts)(spark)
    val filterCondition = filters.reduceLeftOption(And)
    val (planStats, filterPlan) = {
      val qLogRel = new QflockLogicalRelationWithStats(
        relationArgs.relation.asInstanceOf[BaseRelation],
        relationArgs.output,
        relationArgs.catalogTable, false)()
      val filterPlan = filterCondition.map(QflockFilter(_, qLogRel)).getOrElse(qLogRel)
      val fStats = BasicStatsPlanVisitor.visit(filterPlan)
      val projPlan = Project(project, filterPlan)
      (BasicStatsPlanVisitor.visit(projPlan), qLogRel)
    }
    val (relationSizeInBytes, relationRows) = {
      // Just add the size required by the filter to the size required by the project.
      val (filterRelationBytes, relationRows) = if (filterReferences.length > 0) {
        val filterRelationV = new QflockLogicalRelationWithStats(
          qflockRelation.asInstanceOf[BaseRelation],
          filterReferences.distinct,
          relationArgs.catalogTable, false)()
        val relationRowCount = BasicStatsPlanVisitor.visit(filterRelationV).rowCount
        val filterRelation = new QflockLogicalRelation(
          qflockRelation.asInstanceOf[BaseRelation],
          filterReferences.distinct,
          relationArgs.catalogTable, false)(relationRowCount)
        //        logger.warn(s"filterRelationRowCount: ${relationRowCount} " +
        //                    s"filterSize: ${filterRelation.sizeInBytes}")
        (BigInt(filterRelation.sizeInBytes), relationRowCount.get)
      } else {
        // No filter needed, let's just not consider the size of the filter.
        val relationForVisit = new QflockLogicalRelationWithStats(
          qflockRelation.asInstanceOf[BaseRelation],
          attrReferences,
          relationArgs.catalogTable, false)()
        val relationRowCount = BasicStatsPlanVisitor.visit(relationForVisit).rowCount
        (BigInt(0L), relationRowCount.get)
      }
      val refs = QflockUtils.distinctReferences(attrReferences, filterReferences)
      // This is the size required by the project,
      // but less any columns that are also needed by the filter since
      // those columns are already included in the filter.
      val projectRelationBytes = {
        val qLogRel = new QflockLogicalRelation(
          qflockRelation.asInstanceOf[BaseRelation],
          // attrReferences.distinct.filter(x => !filterReferences.contains(x)),
          refs,
          relationArgs.catalogTable, false) (Some(relationRows)) // (planStats.rowCount)
        //      logger.warn(s"projectBytes: ${qLogRel.sizeInBytes} refs: ${refs.length}" +
        //                  s"projectRows: ${planStats.rowCount}")
        qLogRel.sizeInBytes
      }
      // (projectRelationBytes + filterRelationBytes, relationRows)
      (projectRelationBytes, relationRows)
    }
    val scanRelation = new QflockLogicalRelation(qflockRelation.asInstanceOf[BaseRelation],
      references, relationArgs.catalogTable,
      false)(planStats.rowCount,
      relationSizeInBytes, relationRows, Some(!needsPushdown))
    (filterCondition, scanRelation)
  }

  private def pushFilterProject(plan: LogicalPlan): LogicalPlan = {
    val newPlan = plan.transform {
      case s@ScanOperation(project,
      filters,
      child: DataSourceV2ScanRelation) if (needsRule(project, filters, child) &&
        canHandlePlan(project, filters, child)) =>
        val modified = transformProject(s, project, filters, child)
        logger.info("before pushFilterProject: \n" + project + "\n" + s)
        logger.info("after pushFilterProject: \n" + modified)
        modified
      case s@ScanOperation(project,
        filters, child: LogicalRelation) if (needsRule(project, filters, child) &&
        canHandlePlan(project, filters, child)) =>
      val modified = transformProject(s, project, filters, child)
      logger.info("before pushFilterProject: \n" + project + "\n" + s)
      logger.info("after pushFilterProject: \n" + modified)
      modified
    }
    if (newPlan != plan) {
      // newPlan.invalidateStatsCache()
      logger.info(s"new stats: ${newPlan.stats}")
      logger.info("before: \n" + plan)
      logger.info("after: \n" + newPlan)
    }
    newPlan
  }
  protected val logger = LoggerFactory.getLogger(getClass)
  def apply(inputPlan: LogicalPlan): LogicalPlan = {
    // val after = pushAggregate(pushFilterProject(inputPlan))
    val after = pushFilterProject(inputPlan)
    after
  }
}

object QflockExplainOptimizationRule extends Rule[LogicalPlan] {
  val spark: SparkSession =
    SparkSession.builder().appName("Extra optimization rules")
      .getOrCreate()
  def apply(logicalPlan: LogicalPlan): LogicalPlan = {
    QflockExplainRule(spark).apply(logicalPlan)
  }
}
object QflockExplainRuleBuilder {
  var injected: Boolean = false
  protected val logger = LoggerFactory.getLogger(getClass)
  def injectExtraOptimization(): Unit = {
    val testSparkSession: SparkSession =
      SparkSession.builder().appName("Extra optimization rules")
        .getOrCreate()
    import testSparkSession.implicits._
    testSparkSession.experimental.extraOptimizations = Seq(QflockExplainOptimizationRule)

    logger.info(s"added QflockExplainOptimizationRule to session ${testSparkSession}")
  }
}

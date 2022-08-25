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

import scala.annotation.tailrec
import scala.util.{Either, Left => EitherLeft, Right => EitherRight}

import com.github.qflock.extensions.common.PushdownSqlStatus
import com.github.qflock.extensions.jdbc.QflockJdbcScan
import org.slf4j.{Logger, LoggerFactory}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter => LogicalFilter}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation


case class QflockExplainRule(spark: SparkSession) extends Rule[LogicalPlan] {
  protected val appId: String = spark.sparkContext.applicationId

  @tailrec
  private def getAttribute(origExpression: Any) : Either[String, AttributeReference] = {
    origExpression match {
      case Alias(child, _) =>
        getAttribute(child)
      case Cast(expression, _, _, _) =>
        getAttribute(expression)
      case attrib: AttributeReference =>
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
      if (attrSeq.isEmpty) {
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
      case attrib: AttributeReference =>
        Seq(attrib)
      case Cast(expression, _, _, _) =>
        getFilterExpressionAttributes(expression)
      case Or(left, right) => getFilterExpressionAttributes(left) ++
        getFilterExpressionAttributes(right)
      case And(left, right) => getFilterExpressionAttributes(left) ++
        getFilterExpressionAttributes(right)
      case Not(filter) => getFilterExpressionAttributes(filter)
      case In(attr, _) => getFilterExpressionAttributes(attr)
      case EqualTo(attr, _) => getFilterExpressionAttributes(attr)
      case LessThan(attr, _) => getFilterExpressionAttributes(attr)
      case GreaterThan(attr, _) => getFilterExpressionAttributes(attr)
      case LessThanOrEqual(attr, _) => getFilterExpressionAttributes(attr)
      case GreaterThanOrEqual(attr, _) => getFilterExpressionAttributes(attr)
      case IsNull(attr) => getFilterExpressionAttributes(attr)
      case IsNotNull(attr) => getFilterExpressionAttributes(attr)
      case StartsWith(left, _) => getFilterExpressionAttributes(left)
      case EndsWith(left, _) => getFilterExpressionAttributes(left)
      case Contains(left, _) => getFilterExpressionAttributes(left)
      case other@_ => logger.warn("unknown filter:" + other) ; Seq[AttributeReference]()
    }
  }
  private def needsRule(child: Any): Boolean = {
    child match {
      case DataSourceV2ScanRelation(_, scan, _, _) =>
        !scan.isInstanceOf[QflockJdbcScan]
      case qlr@QflockLogicalRelation(relation, _, _, _) =>
        relation match {
          // If we injected it just for size estimates, allow it to continue.
          case _: QflockRelation if qlr.isEstimate => true
          case _: QflockRelation => false
          case _ => true
        }
      case LogicalRelation(_, _, _, _) => true
      case _ => false
    }
  }
  def canHandlePlan(project: Seq[NamedExpression],
                    filters: Seq[Expression],
                    child: Any,
                    alwaysInject: Boolean = true): Boolean = {
    val relationArgsOpt = QflockRelationArgs(child)
    if (relationArgsOpt.isEmpty) {
      return false
    }
//    val relationArgs = relationArgsOpt.get
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
        //      } else if (false && !filters.exists(x => !x.isInstanceOf[IsNotNull])) {
        //        // We only pushdown if there are some filters that are not (IsNotNull).
        //        // logger.warn("Plan has no filters ")
        //        relationArgs.scan match {
        //            case QflockRelation(schema, parts, opts) => false
        //            case _ => alwaysInject
        //        }
        //      }
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
    val relationArgs = QflockRelationArgs(child).get
    val attrReferencesEither = getAttributeReferences(project)

    val attrReferences = attrReferencesEither match {
      case EitherRight(r) => r
      case EitherLeft(_) => Seq[AttributeReference]()
    }
    val filterReferencesEither = getFilterAttributes(filters)
    val filterReferences = filterReferencesEither match {
      case EitherRight(r) => r
      case EitherLeft(_) => Seq[AttributeReference]()
    }
    val opt = new util.HashMap[String, String](relationArgs.options)
    val path = opt.get("path") // .replaceFirst("hdfs://.*:9000/", "hdfs://dikehdfs:9860/")
    opt.put("path", path)
    opt.put("format", "parquet")
    opt.put("outputFormat", "binary")
    val filtersStatus = PushdownSqlStatus.FullyValid
    val references = {
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
    if (withFilter.output != project || filters.isEmpty) {
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
  private def pushFilterProject(plan: LogicalPlan): LogicalPlan = {
    val newPlan = plan.transform {
      case s@ScanOperation(project,
      filters,
      child: DataSourceV2ScanRelation) if needsRule(child) &&
        canHandlePlan(project, filters, child) =>
        val modified = transformProject(s, project, filters, child)
        logger.info("before pushFilterProject: \n" + project + "\n" + s)
        logger.info("after pushFilterProject: \n" + modified)
        modified
      case s@ScanOperation(project,
        filters, child: LogicalRelation) if needsRule(child) &&
        canHandlePlan(project, filters, child) =>
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
  protected val logger: Logger = LoggerFactory.getLogger(getClass)
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
  protected val logger: Logger = LoggerFactory.getLogger(getClass)
  def injectExtraOptimization(): Unit = {
    val testSparkSession: SparkSession =
      SparkSession.builder().appName("Extra optimization rules")
        .getOrCreate()
    import testSparkSession.implicits._
    testSparkSession.experimental.extraOptimizations = Seq(QflockExplainOptimizationRule)

    logger.info(s"added QflockExplainOptimizationRule to session $testSparkSession")
  }
}

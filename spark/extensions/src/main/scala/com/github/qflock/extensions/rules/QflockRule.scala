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

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.{Either, Left => EitherLeft, Right => EitherRight}

import com.github.qflock.extensions.common.{PushdownJson, PushdownJsonStatus, PushdownSQL, PushdownSqlStatus}
import com.github.qflock.extensions.jdbc.QflockJdbcScan
import org.json._
import org.slf4j.LoggerFactory

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter => LogicalFilter}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.hive.extension.ExtHiveUtils
import org.apache.spark.sql.types._


/** This rule injects our jdbc data source in cases where
 *  we can use the jdbc server to efficiently reduce the data
 *  that needs to be transferred.
 * @param spark the current SparkSession
 */
case class QflockRule(spark: SparkSession) extends Rule[LogicalPlan] {
  protected val appId: String = spark.sparkContext.applicationId
  protected var generationId = 0

  @tailrec
  private def getAttribute(origExpression: Any) : Either[String, Option[AttributeReference]] = {
    origExpression match {
      case Alias(child, _) =>
        getAttribute(child)
      case Cast(expression, dataType, timeZoneId, _) =>
        getAttribute(expression)
      case Add(left, right, failOnError) =>
        // @todo For now assume right is constant.
        getAttribute(left)
      case Subtract(left, right, failOnError) =>
        // @todo For now assume right is constant.
        getAttribute(left)
      case Multiply(left, right, failOnError) =>
        // @todo For now assume right is constant.
        getAttribute(left)
      case Divide(left, right, failOnError) =>
        // @todo For now assume right is constant.
        getAttribute(left)
      case attrib @ AttributeReference(name, dataType, nullable, meta) =>
        EitherRight(Some(attrib))
      case Literal(value, dataType) =>
        // Literals are not attributes.
        EitherRight(None)
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
          if (r.isEmpty) {
            None
          } else {
            Seq(r.get)
          }
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
        !scan.isInstanceOf[QflockJdbcScan]
      case qlr@QflockLogicalRelation(relation, output, table, _) =>
        relation match {
          // If we injected it just for size estimates, allow it to continue.
          case q@QflockRelation(_, _, _) if qlr.isEstimate => true
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
    val relationArgsOpt = QflockRelationArgs(child)
    if (relationArgsOpt == None) {
      return false
    }
    if (!PushdownSQL.canHandleFilters(filters)) {
      return false
    }
    val relationArgs = relationArgsOpt.get
    if (relationArgs.catalogTable.isEmpty) {
      return false
    }
    val catalogTable = relationArgs.catalogTable.get
    val tableName = catalogTable.identifier.table
    val dbName = catalogTable.identifier.database.getOrElse("")
    val table = ExtHiveUtils.getTable(dbName, tableName)

    // We only continue with the rule if this table is stored remotely.
    // In the future we could parameterize this
    if (!table.getSd.getLocation.contains("-dc2")) {
      return false
    }
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
        // logger.warn("Plan not modified due to filter")
        alwaysInject
      // } else if (!filters.exists(x => !x.isInstanceOf[IsNotNull])) {
      } else if (!filters.exists(x => !x.isInstanceOf[IsNotNull])) {
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
  private def getNdpRelation(path: String,
                             options: util.HashMap[String, String],
                             schema: String):
                             Option[DataSourceV2Relation] = {
    val url = spark.conf.get("qflockJdbcUrl")
    val df = spark.read
      .format("qflockJdbc")
      .option("driver", "com.github.qflock.jdbc.QflockDriver")
      .option("format", "parquet")
      .option("url", url)
      .option("schema", schema)
      .load(path)
    val logicalPlan = df.queryExecution.optimizedPlan
    logicalPlan match {
      case s@ScanOperation(project,
      filters,
      child: DataSourceV2ScanRelation) =>
        child match {
          case DataSourceV2ScanRelation(relation, scan, output) =>
            Some(relation)
          case _ => None
        }
      case _ => None
    }
  }

  private def transformProject(plan: LogicalPlan,
                               project: Seq[NamedExpression],
                               filters: Seq[Expression],
                               child: LogicalPlan)
  : LogicalPlan = {
    generationId += 1
    val relationArgs = QflockRelationArgs(child).get
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
    val filtersStatus = {
      if (relationArgs.options.containsKey("ndpdisablefilterpush")) {
        PushdownSqlStatus.Invalid
      } else PushdownSQL.validateFilters(filters)
    }
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
    val cols = references.toStructType.fields.map(x => s"" + s"${x.name}").mkString(",")

   /* The below allows us to log the available filters
    * for pushdown, even if we currently do not push these down.
    * These get logged to filters.txt, along with the
    * projects and the Spark view of the filters too.
    */
    if (false) {
      val filtersJson = PushdownJson.getFiltersJsonMaxDesired(filters, "")
      val fw = new FileWriter("/build/filters.txt", true)
      try {
        fw.write("Pushdown Filters " + filters.mkString(", ") + "\n")
        fw.write("Pushdown Projects " + cols + "\n")
        fw.write("Pushdown Filter Json " + filtersJson + "\n")
      }
      finally fw.close()
    }
    val allRefs = attrReferences ++ filterReferences
    val queryCols = allRefs.distinct.toStructType.fields.map(x => s"${x.name}")
    val sqlQuery: String = {
      // For now we always push down
      if (true || filtersStatus != PushdownSqlStatus.Invalid) {
        val pushdownSql = PushdownSQL(references.toStructType, filters, queryCols)
        val query = pushdownSql.query
        logger.warn("Pushdown query " + query)
        query
      } else {
        logger.warn("No Pushdown " + filters.toString)
        ""
      }
    }
    logger.info(s"sqlQuery: $sqlQuery")

    val opt = new util.HashMap[String, String](relationArgs.options)
    val path = opt.get("path")
    opt.put("appId", s"$appId-$generationId")
    opt.put("path", path)
    opt.put("url", spark.conf.get("qflockJdbcUrl"))
    opt.put("format", "parquet")
    opt.put("driver", "com.github.qflock.jdbc.QflockDriver")
    val query = sqlQuery.replace("TABLE_TAG", relationArgs.catalogTable.get.identifier.table)
    opt.put("query", query)
    val catalogTable = relationArgs.catalogTable.get
    val tableName = catalogTable.identifier.table
    val dbName = catalogTable.identifier.database.getOrElse("")
    val table = ExtHiveUtils.getTable(dbName, tableName)
    val rgParamName = s"spark.qflock.statistics.tableStats.$tableName.row_groups"
    opt.put("numRows",
      table.getParameters.get("spark.sql.statistics.numRows"))
    opt.put("numRowGroups",
            table.getParameters.get(rgParamName))
    opt.put("tableName", tableName)
    val schemaStr = catalogTable.schema.fields.map(s =>
      s.dataType match {
        case StringType => s"${s.name}:string:${s.nullable}"
        case IntegerType => s"${s.name}:integer:${s.nullable}"
        case LongType => s"${s.name}:long:${s.nullable}"
        case DoubleType => s"${s.name}:double:${s.nullable}"
        case _ => s""
      }).mkString(",")
    opt.put("schema", schemaStr)

    val filterCondition = filters.reduceLeftOption(And)
    val relationForStats = QflockLogicalRelation.apply(project, filterCondition,
                                                       relationArgs, attrReferences,
                                                       filterReferences, opt,
                                                       references, spark)
    opt.put("queryStats", relationForStats.toString)
    val hdfsScanObject = new QflockJdbcScan(references.toStructType, opt,
      relationForStats.toPlanStats(relationArgs.catalogTable.get.stats.get))
    val ndpRel = getNdpRelation(path, opt, schemaStr)
    val scanRelation = DataSourceV2ScanRelation(ndpRel.get, hdfsScanObject, references)
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
  private def pushFilterProject(plan: LogicalPlan): LogicalPlan = {
    val newPlan = plan.transform {
      case s@ScanOperation(project,
      filters,
      child: DataSourceV2ScanRelation) if needsRule(project, filters, child) &&
        canHandlePlan(project, filters, child) =>
        val modified = transformProject(s, project, filters, child)
        logger.info("before pushFilterProject: \n" + project + "\n" + s)
        logger.info("after pushFilterProject: \n" + modified)
        modified
      case s@ScanOperation(project,
        filters, child: LogicalRelation) if needsRule(project, filters, child) &&
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
  private def transformAggregate(groupingExpressions: Seq[Expression],
                                 aggregateExpressions: Seq[NamedExpression],
                                 child: LogicalPlan)
  : LogicalPlan = {
    val relationArgs = QflockRelationArgs(child).get
    val aggExprToOutputOrdinal = mutable.HashMap.empty[Expression, Int]
    var ordinal = 0
    val aggregates = aggregateExpressions.flatMap { expr =>
      expr.collect {
        // Do not push down duplicated aggregate expressions. For example,
        // `SELECT max(a) + 1, max(a) + 2 FROM ...`, we should only push down one
        // `max(a)` to the data source.
        case agg: AggregateExpression
          if !aggExprToOutputOrdinal.contains(agg.canonicalized) =>
          aggExprToOutputOrdinal(agg.canonicalized) = ordinal
          ordinal += 1
          agg
      }
    }
    val schema = PushdownJson.getAggregateSchema(aggregates, groupingExpressions)
    val newOutput = schema.map(f => AttributeReference(f.name, f.dataType,
      f.nullable, f.metadata)())
    // assert(newOutput.length == groupingExpressions.length + aggregates.length)
    val groupAttrs = groupingExpressions.zip(newOutput).map {
      case (a: Attribute, b: Attribute) => b.withExprId(a.exprId)
      case (_, b) => b
    }
    val output = groupAttrs ++ newOutput.drop(groupAttrs.length)

    /* logInfo(
      s"""
          |Pushing operators to ${sHolder.relation.name}
          |Pushed Aggregate Functions:
          | ${pushedAggregates.get.aggregateExpressions.mkString(", ")}
          |Pushed Group by:
          | ${pushedAggregates.get.groupByColumns.mkString(", ")}
          |Output: ${output.mkString(", ")}
          """.stripMargin) */
    val opt = new HashMap[String, String](relationArgs.options)
    val aggregateJson = PushdownJson.getAggregateJson(groupingExpressions,
      aggregates,
      "")
    opt.put("ndpjsonaggregate", aggregateJson)
    val hdfsScanObject = QflockJdbcScan(output.toStructType, opt)
    val scanRelation = DataSourceV2ScanRelation(
      relationArgs.scan.asInstanceOf[DataSourceV2Relation],
      hdfsScanObject, output)
    val plan = Aggregate(
      output.take(groupingExpressions.length),
      aggregateExpressions, scanRelation)
    val aggOutput = output.drop(groupAttrs.length)
    plan.transformExpressions {
      case agg: AggregateExpression =>
        val ordinal = aggExprToOutputOrdinal(agg.canonicalized)
        val aggFunction: aggregate.AggregateFunction =
          agg.aggregateFunction match {
            case max: aggregate.Max => max.copy(child = aggOutput(ordinal))
            case min: aggregate.Min => min.copy(child = aggOutput(ordinal))
            case sum: aggregate.Sum => sum.copy(child = aggOutput(ordinal))
            case _: aggregate.Count => aggregate.Sum(aggOutput(ordinal))
            case other => other
          }
        agg.copy(aggregateFunction = aggFunction)
    }
    // plan
  }
  private def aggNeedsRule(plan: LogicalPlan): Boolean = {
    plan match {
      case s@ScanOperation(project,
      filters,
      child: DataSourceV2ScanRelation) =>
        val relationScan = child match {
          case DataSourceV2ScanRelation(relation, scan, output) =>
            scan
        }
        val scanOpts = relationScan match {
          case ParquetScan(_, _, _, dataSchema, _, _, _, opts, _, _) =>
            opts
          case QflockJdbcScan(schema, opts, stats) =>
            opts
        }
        !scanOpts.containsKey("ndpjsonaggregate") &&
          !scanOpts.containsKey("ndpdisableaggregatepush")
      case _ => false
    }
  }
  private def aggExpressionIsValid(groupingExpressions: Seq[Expression],
                                   aggregateExpressions: Seq[NamedExpression]): Boolean = {
    def aggValidateExpression(expr: Expression): Boolean = {
      expr match {
        case attrib @ AttributeReference(name, dataType, nullable, meta) =>
          true
        case Literal(value, dataType) =>
          true
        case s @ ScalaUDF(function, dataType, children, inputEncoders, outputEncoder,
        udfName, nullable, udfDeterministic) =>
          // logger.info(s"$function, $dataType, $children, " +
          //              "$inputEncoders, $outputEncoder, $udfName")
          logger.info("aggregate UDF not supported")
          false
        /* case If(predicate, trueValue, falseValue) =>
          // aggValidateExpression(trueValue) && aggValidateExpression(falseValue)
          logger.info("aggregate If not supported")
          false
        case Or(left, right) =>
          // aggValidateExpression(left) && aggValidateExpression(right)
          logger.info("aggregate OR not supported")
          false
        case And(left, right) =>
          // aggValidateExpression(left) && aggValidateExpression(right)
          logger.info("aggregate And not supported")
          false */
        case mult @ Multiply(left, right, failOnError) =>
          aggValidateExpression(left) && aggValidateExpression(right)
        case div @ Divide(left, right, failOnError) =>
          aggValidateExpression(left) && aggValidateExpression(right)
        case add @ Add(left, right, failOnError) =>
          aggValidateExpression(left) && aggValidateExpression(right)
        case subtract @ Subtract(left, right, failOnError) =>
          aggValidateExpression(left) && aggValidateExpression(right)
        case other @ _ =>
          logger.info(s"aggregate ${other.toString} not supported")
          false
      }
    }
    def validateAggFunction(aggregate: AggregateExpression): Boolean = {
      if (aggregate.filter.isEmpty) {
        aggregate.aggregateFunction match {
          case Min(child: Expression) => aggValidateExpression(child)
          case Max(child: Expression) => aggValidateExpression(child)
          case count: Count if count.children.length == 1 =>
            aggValidateExpression(count.children.head)
          case sum @ Sum(child: Expression, _) => aggValidateExpression(child)
          case _ => false
        }
      } else {
        false
      }
    }
    val aggregates = aggregateExpressions.flatMap { expr =>
      expr.collect {
        // Do not push down duplicated aggregate expressions. For example,
        // `SELECT max(a) + 1, max(a) + 2 FROM ...`, we should only push down one
        // `max(a)` to the data source.
        case agg: AggregateExpression => agg
      }
    }
    !aggregates.exists(x => !validateAggFunction(x))
  }

  private def pushAggregate(plan: LogicalPlan)
  : LogicalPlan = {
    val newPlan = plan.transform {
      case aggNode @ Aggregate(groupingExpressions, resultExpressions, childAgg)
        if (false && aggExpressionIsValid(groupingExpressions, resultExpressions) &&
          aggNeedsRule(childAgg)) =>
        childAgg match {
          case s@ScanOperation(project,
          filters,
          child: DataSourceV2ScanRelation)
            if filters.isEmpty =>
            transformAggregate(groupingExpressions,
              resultExpressions, child)
          case r: DataSourceV2ScanRelation =>
            aggNode
          case other =>
            aggNode
        }
    }
    if (newPlan != plan) {
      logger.info("before agg pushdown: \n" + plan)
      logger.info("after agg pushdown: \n" + newPlan)
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
object QflockOptimizationRule extends Rule[LogicalPlan] {
  val spark: SparkSession =
    SparkSession.builder().appName("Extra optimization rules")
      .getOrCreate()
  def apply(logicalPlan: LogicalPlan): LogicalPlan = {
    QflockRule(spark).apply(logicalPlan)
  }
}
object QflockRuleBuilder {
  var injected: Boolean = false
  protected val logger = LoggerFactory.getLogger(getClass)
  def injectExtraOptimization(): Unit = {
    val testSparkSession: SparkSession =
      SparkSession.builder().appName("Extra optimization rules")
        .getOrCreate()
    import testSparkSession.implicits._
    logger.info(s"added QflockOptimizationRule to session ${testSparkSession}")
    testSparkSession.experimental.extraOptimizations = Seq(QflockOptimizationRule)
  }
}

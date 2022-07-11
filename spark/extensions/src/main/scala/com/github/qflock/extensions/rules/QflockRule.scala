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
import scala.collection.mutable
import scala.util.{Either, Left => EitherLeft, Right => EitherRight}

import com.github.qflock.extensions.common.{PushdownSQL, PushdownSqlStatus, QflockQueryCache}
import com.github.qflock.extensions.jdbc.{QflockDataSourceV2ScanRelation, QflockJdbcScan, QflockLog}
import org.slf4j.{Logger, LoggerFactory}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftOuter, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter => LogicalFilter}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.hive.extension.ExtHiveUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap


/** This rule injects our jdbc data source in cases where
 *  we can use the jdbc server to efficiently reduce the data
 *  that needs to be transferred.
 * @param spark the current SparkSession
 */
case class QflockRule(spark: SparkSession) extends Rule[LogicalPlan] {
  protected val appId: String = spark.sparkContext.applicationId

  @tailrec
  private def getAttribute(origExpression: Any) : Either[String, Option[AttributeReference]] = {
    origExpression match {
      case Alias(child, _) =>
        getAttribute(child)
      case Cast(expression, _, _, _) =>
        getAttribute(expression)
      case Add(left, _, _) =>
        // @todo For now assume right is constant.
        getAttribute(left)
      case Subtract(left, _, _) =>
        // @todo For now assume right is constant.
        getAttribute(left)
      case Multiply(left, _, _) =>
        // @todo For now assume right is constant.
        getAttribute(left)
      case Divide(left, _, _) =>
        // @todo For now assume right is constant.
        getAttribute(left)
      case attrib @ AttributeReference(_, _, _, _) =>
        EitherRight(Some(attrib))
      case Literal(_, _) =>
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
  private def needsUpdate(project: Seq[NamedExpression],
                          child: Any): Boolean = {
    val projectValid = project.map {
      case _: AttributeReference => true
      case _ => false
    }.exists(a => a)
    if (!projectValid) {
      return false
    }
    child match {
      case DataSourceV2ScanRelation(_, scan, output) =>
        val isJoin = scan match {
          case QflockJdbcScan(_, _, statsParam, _)
            if statsParam.isDefined && statsParam.get.isInstanceOf[QflockJoinStatsParameters] =>
              true
          case _ => false
        }
        // Only consider it a match if it is not a join.
        if (scan.isInstanceOf[QflockJdbcScan] &&
            (output.length != project.length) && !isJoin) {
          true
        } else {
          false
        }
      case _ => false
    }
  }
  private def needsJoinUpdate(project: Seq[NamedExpression],
                              child: Any): Boolean = {
    val projectValid = project.map {
      case _: AttributeReference => true
      case _ => false
    }.exists(a => a)
    if (!projectValid) {
      return false
    }
    child match {
      case DataSourceV2ScanRelation(_, scan, output) =>
        val isJoin = scan match {
          case QflockJdbcScan(_, _, statsParam, _)
            if statsParam.isDefined && statsParam.get.isInstanceOf[QflockJoinStatsParameters] =>
                true
          case _ => false
        }
        // Only consider it a match if it is a join.
        if (scan.isInstanceOf[QflockJdbcScan] &&
          (output.length != project.length) && isJoin) {
          true
        } else {
          false
        }
      case _ => false
    }
  }
  private def needsRule(child: Any): Boolean = {
    child match {
      case DataSourceV2ScanRelation(_, scan, _) =>
        !scan.isInstanceOf[QflockJdbcScan]
      case qlr@QflockLogicalRelation(relation, _, _, _) =>
        relation match {
          // If we injected it just for size estimates, allow it to continue.
          case _: QflockRelation if qlr.isEstimate => true
          case _: QflockRelation => false
          case _ => true
        }
      case _: LogicalRelation => true
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
      } else if (filters.forall(x => x.isInstanceOf[IsNotNull])) {
        // We only pushdown if there are some filters that are not (IsNotNull).
        // logger.warn("Plan has no filters ")
        relationArgs.scan match {
            case _: QflockRelation => false
            case _ => alwaysInject
        }
      } else {
        true
      }
    }
  }
  private def getNdpRelation(path: String,
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
      case ScanOperation(_,
      _,
      child: DataSourceV2ScanRelation) =>
        child match {
          case DataSourceV2ScanRelation(relation, _, _) =>
            Some(relation)
          case _ => None
        }
      case _ => None
    }
  }

  private def transformProject(project: Seq[NamedExpression],
                               filters: Seq[Expression],
                               child: LogicalPlan)
  : LogicalPlan = {
    val generationId = QflockOptimizationRule.getGenerationId
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
    val allRefs = attrReferences ++ filterReferences
    val queryCols = allRefs.distinct.toStructType.fields.map(x => s"${x.name}")
    val sqlQuery: String = {
      // For now we always push down
      if (filtersStatus != PushdownSqlStatus.Invalid) {
        val pushdownSql = PushdownSQL(references.toStructType, filters, queryCols)
        val query = pushdownSql.query
        // logger.info("Pushdown query " + query)
        query
      } else {
        // logger.info("No Pushdown " + filters.toString)
        ""
      }
    }
    // logger.info(s"sqlQuery: $sqlQuery")

    val opt = new util.HashMap[String, String](relationArgs.options)
    opt.put("rulelog", opt.getOrDefault("rulelog", "") + "projectfilter,")
    if (filters.nonEmpty) {
      opt.put("rulelog", opt.getOrDefault("rulelog", "") + "hasfilters,")
    }
    val path = opt.get("path")
    val testNum = spark.conf.get("qflockTestNum")
    val fullAppId = s"$appId$testNum-$generationId"
    opt.put("appid", fullAppId)
    opt.put("path", path)
    opt.put("url", spark.conf.get("qflockJdbcUrl"))
    opt.put("resultspath", spark.conf.get("qflockResultsPath", "data"))
    opt.put("queryname", spark.conf.get("qflockQueryName"))
    opt.put("format", "parquet")
    opt.put("driver", "com.github.qflock.jdbc.QflockDriver")
    val query = sqlQuery.replace("TABLE_TAG", relationArgs.catalogTable.get.identifier.table)
    opt.put("query", query)
    val catalogTable = relationArgs.catalogTable.get
    val tableName = catalogTable.identifier.table
    val dbName = catalogTable.identifier.database.getOrElse("")
    val table = ExtHiveUtils.getTable(dbName, tableName)
    val rgParamName = s"spark.qflock.statistics.tableStats.$tableName.row_groups"
    opt.put("numrows",
      table.getParameters.get("spark.sql.statistics.numRows"))
    val numRowGroups = table.getParameters.get(rgParamName)
    opt.put("numrowgroups", numRowGroups)
    opt.put("tablename", tableName)
    val schemaStr = catalogTable.schema.fields.map(s =>
      s.dataType match {
        case StringType => s"${s.name}:string:${s.nullable}"
        case IntegerType => s"${s.name}:integer:${s.nullable}"
        case LongType => s"${s.name}:long:${s.nullable}"
        case DoubleType => s"${s.name}:double:${s.nullable}"
        case _ => s""
      }).mkString(",")
    opt.put("schema", schemaStr)

    /* Prepare for later caching if needed. */
    QflockQueryCache.addKey(query)
    val filterCondition = filters.reduceLeftOption(And)
    val statsParameters = QflockStatsParameters(project, filterCondition,
                                                relationArgs, attrReferences,
                                                filterReferences, opt,
                                                references)
    val relationForStats = QflockLogicalRelation.apply(project, filterCondition,
                                                       relationArgs, attrReferences,
                                                       filterReferences, opt,
                                                       references, spark)
//    opt.put("queryStats", relationForStats.toString)
    val hdfsScanObject = QflockJdbcScan(references.toStructType, opt,
      Some(statsParameters),
      relationForStats.toPlanStats(relationArgs.catalogTable.get.stats.get))
    val ndpRel = getNdpRelation(path, schemaStr)
    val scanRelation = new QflockDataSourceV2ScanRelation(ndpRel.get, hdfsScanObject, references,
      relationArgs.catalogTable.get)
//    val scanRelation = DataSourceV2ScanRelation(ndpRel.get, hdfsScanObject, references)
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
  private def updateProject(project: Seq[NamedExpression],
                            filters: Seq[Expression],
                            child: LogicalPlan)
  : LogicalPlan = {
    val relationArgs = QflockRelationArgs(child).get
    val attrReferencesEither = getAttributeReferences(project)

    val attrReferences = attrReferencesEither match {
      case EitherRight(r) => r
      case EitherLeft(_) => Seq[AttributeReference]()
    }
    val opt = new util.HashMap[String, String](relationArgs.options)
    val query = opt.get("query")
    val trimmedQuery = query.split("FROM").drop(1).mkString("FROM")
    val newSelect = project.map(x => x.name).mkString(",")
    val newQuery = s"SELECT $newSelect FROM $trimmedQuery"
    opt.put("query", newQuery)
    opt.put("rulelog", opt.getOrDefault("rulelog", "") + "updateproject,")
    val references = attrReferences.distinct
    val statsParam = relationArgs.statsParam.get.asInstanceOf[QflockStatsParameters]
    val statsParameters = QflockStatsParameters(
      project, statsParam.filterCondition,
      statsParam.relationArgs, attrReferences,
      statsParam.filterReferences, opt,
      references)
    val relationForStats = QflockLogicalRelation.apply(project,
      statsParam.filterCondition,
      statsParam.relationArgs, attrReferences,
      statsParam.filterReferences, opt,
      references, spark)
    //    opt.put("queryStats", relationForStats.toString)
    val hdfsScanObject = QflockJdbcScan(references.toStructType, opt,
      Some(statsParameters),
      relationForStats.toPlanStats(statsParam.relationArgs.catalogTable.get.stats.get))
    val ndpRel = getNdpRelation(opt.get("path"), opt.get("schema"))
    val scanRelation = DataSourceV2ScanRelation(ndpRel.get, hdfsScanObject, references)
    val withFilter = {
      if (true) {
        /* Clip the filter from the DAG, since we are going to
         * push down the entire filter to NDP.
         */
        scanRelation
      } else {
        statsParam.filterCondition.map(LogicalFilter(_, scanRelation)).getOrElse(scanRelation)
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
  private def updateJoinProject(project: Seq[NamedExpression],
                                filters: Seq[Expression],
                                child: LogicalPlan)
  : LogicalPlan = {
    val relationArgsProject = QflockRelationArgs(child).get
    val attrReferencesEither = getAttributeReferences(project)
    val attrReferences = attrReferencesEither match {
      case EitherRight(r) => r
      case EitherLeft(_) => Seq[AttributeReference]()
    }
    val opt = new util.HashMap[String, String](relationArgsProject.options)
    val query = opt.get("query")
    val trimmedQuery = query.split("FROM").drop(1).mkString("FROM")
    val newSelect = project.map(x => x.name).mkString(",")
    val newQuery = s"SELECT $newSelect FROM $trimmedQuery"
    opt.put("query", newQuery)
    val updateType = if (query.contains("JOIN")) "updatejoin," else "updateproject,"
    opt.put("rulelog", opt.getOrDefault("rulelog", "") + updateType)
    val references = attrReferences.distinct
    val statsParamsOrig = relationArgsProject.statsParam.get.asInstanceOf[QflockJoinStatsParameters]
    val join = statsParamsOrig.join
    val relationArgs =
      relationArgsProject.statsParam.get.asInstanceOf[QflockJoinStatsParameters].relationArgs
    val statsParameters = QflockJoinStatsParameters(relationArgs, opt, references, join,
      statsParamsOrig.catalogTables)
    val relationArgsJoin = new QflockRelationArgs(relationArgs.relation,
      relationArgs.scan, references,
      references.toStructType, references.toStructType,
      new CaseInsensitiveStringMap(opt),
      Some(statsParameters), relationArgs.catalogTable)
    val relationForStats = QflockJoinRelation.apply(relationArgsJoin,
      statsParameters.join, opt, references, spark)
    //    opt.put("queryStats", relationForStats.toString)
    val hdfsScanObject = QflockJdbcScan(references.toStructType, opt,
      Some(statsParameters),
      relationForStats.toPlanStats(relationArgsJoin.catalogTable.get.stats.get))
    val ndpRel = getNdpRelation(opt.get("path"), opt.get("schema"))
    val scanRelation = DataSourceV2ScanRelation(ndpRel.get, hdfsScanObject, references)
    scanRelation
  }
  private def pushFilterProject(plan: LogicalPlan): LogicalPlan = {
    val newPlan = plan.transform {
      case s@ScanOperation(project,
      filters,
      child: DataSourceV2ScanRelation) if needsJoinUpdate(project, child) =>
        val modified = updateJoinProject(project, filters, child)
        logger.info("before updateJoinProject: \n" + project + "\n" + s)
        logger.info("after updateJoinProject: \n" + modified)
        modified
      case s@ScanOperation(project,
      filters,
      child: DataSourceV2ScanRelation) if needsUpdate(project, child) =>
        val modified = updateProject(project, filters, child)
        logger.info("before updateFilterProject: \n" + project + "\n" + s)
        logger.info("after updateFilterProject: \n" + modified)
        modified
      case s@ScanOperation(project,
      filters,
      child: DataSourceV2ScanRelation) if needsRule(child) &&
        canHandlePlan(project, filters, child) =>
        val modified = transformProject(project, filters, child)
        logger.info("before pushFilterProject: \n" + project + "\n" + s)
        logger.info("after pushFilterProject: \n" + modified)
        modified
      case s@ScanOperation(project,
        filters, child: LogicalRelation) if needsRule(child) &&
        canHandlePlan(project, filters, child) =>
      val modified = transformProject(project, filters, child)
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
    val schema = PushdownSQL.getAggregateSchema(aggregates, groupingExpressions)
    val newOutput = schema.map(f => AttributeReference(f.name, f.dataType,
      f.nullable, f.metadata)())
    // assert(newOutput.length == groupingExpressions.length + aggregates.length)
    val groupAttrs = groupingExpressions.zip(newOutput).map {
      case (a: Attribute, b: Attribute) => b.withExprId(a.exprId)
      case (_, b) => b
    }
    val output = groupAttrs ++ newOutput.drop(groupAttrs.length)
    val opt = new util.HashMap[String, String](relationArgs.options)
    val query = opt.get("query").split("FROM ").drop(1).mkString(" ")
    val newQuery = PushdownSQL.getAggregateSql(aggregates, groupingExpressions, query)
    opt.put("query", newQuery)
    opt.put("aggregatequery", "true")
    opt.put("rulelog", opt.getOrDefault("rulelog", "") + "aggregate,")
    val hdfsScanObject = QflockJdbcScan(output.toStructType, opt,
                                        relationArgs.statsParam)
    val scanRelation = DataSourceV2ScanRelation(
      relationArgs.relation.asInstanceOf[DataSourceV2Relation],
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
  }
  private def aggNeedsRule(plan: LogicalPlan): Boolean = {
    plan match {
      case ScanOperation(_, _,
                         child: DataSourceV2ScanRelation) =>
        val relationScan = child match {
          case DataSourceV2ScanRelation(_, scan, _) =>
            scan
        }
        val scanOpts = relationScan match {
          case ParquetScan(_, _, _, _, _, _, _, opts, _, _) =>
            opts
          case QflockJdbcScan(_, opts, _, _) =>
            opts
        }
        !scanOpts.containsKey("ndpjsonaggregate") &&
          !scanOpts.containsKey("ndpdisableaggregatepush") &&
          !scanOpts.containsKey("aggregatequery")
      case _ => false
    }
  }
  private def aggExpressionIsValid(aggregateExpressions: Seq[NamedExpression]): Boolean = {
    def aggValidateExpression(expr: Expression): Boolean = {
      expr match {
        case _: AttributeReference =>
          true
        case _: Literal =>
          true
        case _: ScalaUDF =>
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
        case Multiply(left, right, _) =>
          aggValidateExpression(left) && aggValidateExpression(right)
        case Divide(left, right, _) =>
          aggValidateExpression(left) && aggValidateExpression(right)
        case Add(left, right, _) =>
          aggValidateExpression(left) && aggValidateExpression(right)
        case Subtract(left, right, _) =>
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
          case count: Count if count.children.length == 1 && !aggregate.isDistinct =>
            aggValidateExpression(count.children.head)
          case Sum(child: Expression, _) => aggValidateExpression(child)
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
    aggregates.forall(x => validateAggFunction(x))
  }

  private def pushAggregate(plan: LogicalPlan)
  : LogicalPlan = {
    val newPlan = plan.transform {
      case aggNode @ Aggregate(groupingExpressions, resultExpressions, childAgg)
        if aggExpressionIsValid(resultExpressions) &&
          aggNeedsRule(childAgg) =>
        childAgg match {
          case ScanOperation(_,
          filters,
          child: DataSourceV2ScanRelation)
            if filters.isEmpty =>
            transformAggregate(groupingExpressions,
              resultExpressions, child)
          case _: DataSourceV2ScanRelation =>
            aggNode
          case _ => aggNode
        }
    }
    if (newPlan != plan) {
      logger.info("before agg pushdown: \n" + plan)
      logger.info("after agg pushdown: \n" + newPlan)
    }
    newPlan
  }
  private def getProjectQuery(project: Seq[NamedExpression],
                              filters: Seq[Expression],
                              child: LogicalPlan)
  : String = {
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
    val allRefs = attrReferences ++ filterReferences
    val queryCols = allRefs.distinct.toStructType.fields.map(x => s"${x.name}")
    val sqlQuery: String = {
      // For now we always push down
      if (filtersStatus != PushdownSqlStatus.Invalid) {
        val pushdownSql = PushdownSQL(references.toStructType, filters, queryCols)
        val query = pushdownSql.query
        // logger.info("Pushdown query " + query)
        query
      } else {
        // logger.info("No Pushdown " + filters.toString)
        ""
      }
    }
    sqlQuery.replace("TABLE_TAG", relationArgs.catalogTable.get.identifier.table)
  }
  // Copied from Spark's PredicateHelper trait in predicates.scala
  def splitAndExpression(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitAndExpression(cond1) ++ splitAndExpression(cond2)
      case other => other :: Nil
    }
  }
  def getJoinQuery(join: LogicalPlan, left: LogicalPlan, right: LogicalPlan,
                   joinType: JoinType,
                   expression: Option[Expression]): String = {
    val references = join.output
    val relationArgsLeft = QflockRelationArgs(left).get
    val relationArgsRight = QflockRelationArgs(right).get
    val queryLeft = left match {
      case s@ScanOperation(project, filters, child: DataSourceV2ScanRelation) =>
        getProjectQuery(project, filters, child)
    }
    val queryRight = right match {
      case s@ScanOperation(project, filters, child: DataSourceV2ScanRelation) =>
        getProjectQuery(project, filters, child)
    }
    val queryCols = references.distinct.toStructType.fields.map(x => s"${x.name}")
    val colsString = queryCols.mkString(",")
    val query = {
      if (expression.isEmpty) {
        s"SELECT $colsString FROM ($queryLeft) ${joinType.sql} JOIN ($queryRight)"
      } else {
        val expressions = splitAndExpression(expression.get)
        val pushdownSql = PushdownSQL(references.toStructType, expressions, queryCols)
        val expressionString = pushdownSql.getFilterString()

        s"SELECT $colsString FROM ($queryLeft) ${joinType.sql} " +
          s"JOIN ($queryRight) ON ($expressionString)"
      }
    }
    query
  }

  def transformJoin(join: LogicalPlan, left: LogicalPlan, right: LogicalPlan,
                    joinType: JoinType,
                    expression: Option[Expression]): LogicalPlan = {
    val generationId = QflockOptimizationRule.getGenerationId
    val relationArgsLeft = QflockRelationArgs(left).get
    val relationArgsRight = QflockRelationArgs(right).get
    val attrReferencesEither = getAttributeReferences(join.output)
    val references = attrReferencesEither match {
      case EitherRight(r) => r
      case EitherLeft(_) => Seq[AttributeReference]()
    }
    val referencesStructType = references.toStructType
    val opt = new util.HashMap[String, String](relationArgsLeft.options)
    opt.put("rulelog", opt.getOrDefault("rulelog", "") + "join,")
    val testNum = spark.conf.get("qflockTestNum")
    val fullAppId = s"$appId$testNum-$generationId"
    opt.put("appid", fullAppId)
    opt.put("url", spark.conf.get("qflockJdbcUrl"))
    opt.put("resultspath", spark.conf.get("qflockResultsPath", "data"))
    opt.put("queryname", spark.conf.get("qflockQueryName"))
    opt.put("format", "parquet")
    opt.put("driver", "com.github.qflock.jdbc.QflockDriver")
    val query = getJoinQuery(join, left, right, joinType, expression)
    opt.put("query", query)
    val catalogTable = relationArgsLeft.catalogTable.get
    val tableName = catalogTable.identifier.table
    val dbName = catalogTable.identifier.database.getOrElse("")
    val table = ExtHiveUtils.getTable(dbName, tableName)
    val rgParamName = s"spark.qflock.statistics.tableStats.$tableName.row_groups"
    opt.put("numrows",
      table.getParameters.get("spark.sql.statistics.numRows"))
    val numRowGroups = 1 // table.getParameters.get(rgParamName)
    opt.put("numrowgroups", numRowGroups.toString)
    opt.put("tablename", tableName)

    val schemaStr = referencesStructType.fields.map(s =>
      s.dataType match {
        case StringType => s"${s.name}:string:${s.nullable}"
        case IntegerType => s"${s.name}:integer:${s.nullable}"
        case LongType => s"${s.name}:long:${s.nullable}"
        case DoubleType => s"${s.name}:double:${s.nullable}"
        case _ => s""
      }).mkString(",")

    opt.put("schema", schemaStr)

    val statsParameters = QflockJoinStatsParameters(relationArgsLeft, opt, references,
      join,
      Some(Seq(relationArgsLeft.catalogTable.get,
        relationArgsRight.catalogTable.get)))
    val relationArgs = new QflockRelationArgs(relationArgsLeft.relation,
      relationArgsLeft.scan, join.output.asInstanceOf[Seq[AttributeReference]],
      references.toStructType, references.toStructType,
      new CaseInsensitiveStringMap(opt),
      Some(statsParameters), relationArgsLeft.catalogTable)
    val relationForStats = QflockJoinRelation.apply(relationArgs,
      join, opt, references, spark)
    val hdfsScanObject = QflockJdbcScan(referencesStructType, opt,
      Some(statsParameters),
      relationForStats.toPlanStats(relationArgsLeft.catalogTable.get.stats.get))
    val ndpRel = getNdpRelation(opt.get("path"), schemaStr)
    val scanRelation = new QflockDataSourceV2ScanRelation(ndpRel.get, hdfsScanObject,
      references,
      relationArgsLeft.catalogTable.get)
    scanRelation
  }

  def checkJoinChild(plan: LogicalPlan): (Boolean, Option[String]) = {
    plan match {
      case s@ScanOperation(project,
      filters,
      child: DataSourceV2ScanRelation) =>
        child match {
          case DataSourceV2ScanRelation(_, scan, output) =>
            if (scan.isInstanceOf[QflockJdbcScan]) {
              scan match {
                case QflockJdbcScan(_, _, params, _ ) =>
                  if (params.isDefined) {
                    params.get match {
                      case s: QflockStatsParameters =>
                        (true, Some(s.relationArgs.catalogTable.get.identifier.table))
                      case u@unknown => (false, None)
                    }

                  } else {
                    (false, None)
                  }
              }
            } else {
              (false, None)
            }
          case _ => (false, None)
        }
      case s@ScanOperation(project, filters, child: LogicalRelation) =>
        child match {
          case LogicalRelation(relation, output, table, _) =>
            (false, Some(table.get.identifier.table))
          case _ => (false, None)
        }
      case _ => (false, None)
    }
  }
  def isJoinTypeValid(joinType: JoinType): Boolean = {
    joinType match {
      case Inner => true
      case LeftSemi => true
      case LeftOuter => true
      case _ => false
    }
  }
  def checkJoin(plan: LogicalPlan): LogicalPlan = {
    val queryName = spark.conf.get("qflockQueryName")
    val resultsPath = spark.conf.get("qflockResultsPath", "data")
    plan.transform {
      case j@Join(left, right, joinType, condition, joinHint) =>
        val (lValid, lTable) = checkJoinChild(left)
        val (rValid, rTable) = checkJoinChild(right)
        if (rValid && lValid) {
          QflockLog.log(s"queryName:$queryName joinStatus:valid " +
                        s"tables:${rTable.get},${lTable.get} " +
                        s"type:$joinType " + s"hint:$joinHint " +
                        s"condition:$condition " + s"plan:${j.toString}",
                        path = resultsPath)
        } else if (lTable.isDefined && rTable.isDefined) {
          QflockLog.log(s"queryName:$queryName joinStatus:invalid " +
                        s"tables:${rTable.get},${lTable.get} " +
                        s"type:$joinType " + s"hint:$joinHint " +
                        s"condition:$condition " + s"plan:${j.toString}",
                        path = resultsPath)
        }
        j
    }
  }

  def joinValid(left: LogicalPlan, right: LogicalPlan, joinType: JoinType): Boolean = {
    val (lValid, _) = checkJoinChild(left)
    val (rValid, _) = checkJoinChild(right)
    val joinValid = isJoinTypeValid(joinType)
    if (rValid && lValid && joinValid) {
      true
    } else {
      false
    }
  }
  def pushJoin(plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      case j@Join(left, right, joinType, condition, joinHint)
        if joinValid(left, right, joinType) =>
        transformJoin(j, left, right, joinType, condition)
    }
  }
  protected val logger: Logger = LoggerFactory.getLogger(getClass)
  def apply(inputPlan: LogicalPlan): LogicalPlan = {
    val after = pushJoin(pushAggregate(pushFilterProject(inputPlan)))
//    val after = pushAggregate(pushFilterProject(inputPlan))
//    val after = pushFilterProject(inputPlan)
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
  private var generationId: Int = 0

  def getGenerationId: Int = {
    generationId += 1
    generationId
  }
}
object QflockRuleBuilder {
  var injected: Boolean = false
  protected val logger: Logger = LoggerFactory.getLogger(getClass)
  def injectExtraOptimization(): Unit = {
    val testSparkSession: SparkSession =
      SparkSession.builder().appName("Extra optimization rules")
        .getOrCreate()
    // import testSparkSession.implicits._
    logger.info(s"added QflockOptimizationRule to session $testSparkSession")
    testSparkSession.experimental.extraOptimizations = Seq(QflockOptimizationRule)
  }
}

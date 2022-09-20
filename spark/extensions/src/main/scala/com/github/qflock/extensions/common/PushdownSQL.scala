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
/*
 * Note that this code was inspired by the spark-select code:
 *  https://github.com/minio/spark-select/blob/master/src/main/scala
 *         /io/minio/spark/select/FilterPushdown.scala
 *
 * Other portions of this code, most notably compileAggregates, and getColumnSchema,
 * were inspired by this patch by Huaxin Gao:
 *   https://github.com/apache/spark/pull/29695
 */
package com.github.qflock.extensions.common

import java.io.StringWriter
import java.sql.Timestamp
import javax.json.{Json, JsonArrayBuilder}

import com.github.qflock.extensions.common.PushdownSqlStatus.PushdownSqlStatus
import org.slf4j.{Logger, LoggerFactory}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.execution.datasources.PushableColumnWithoutNestedColumn
import org.apache.spark.sql.types._



/** Provides pushdown capabilities aimed at
 *  generating information needed for pushdown
 *  from the inputs that Spark provides.
 *
 * @param schema - StructType of fields in query.
 * @param filters - Filters list.
 * @param queryCols - Array of column names.
 * @param referenceMap [Optional] maps an attribute to a prefix to be used when referencing.
 */
class PushdownSQL(schema: StructType,
                  filters: Seq[Expression],
                  queryCols: Array[String],
                  referenceMap: Map[String, Seq[AttributeReference]]) {

  protected val logger: Logger = LoggerFactory.getLogger(getClass)
  protected val validFilters: Seq[Expression] =
    filters.filter(f => PushdownSQL.validateFilterExpression(f))

  /**
   * Build a string for a set of filters.
   *
   */
  def getFilterString: String = {
    validFilters.flatMap(f => buildFilterExpression(f)).mkString(" AND ")
  }

  /**
   * Build a SQL WHERE clause for the given filters. If a filter cannot be pushed down then no
   * condition will be added to the WHERE clause. If none of the filters can be pushed down then
   * an empty string will be returned.
   *
   */
  def buildWhereClause(): String = {
    val filterExpressions = getFilterString
    if (filterExpressions.isEmpty) "" else "WHERE " + filterExpressions
  }
  /**
   * Attempt to convert the given filter into a Select expression. Returns None if the expression
   * could not be converted.
   */
  def buildFilterExpression(filter: Expression): Option[String] = {
    def buildComparison(expr1: Expression,
                        expr2: Expression,
                        comparisonOp: String): Option[String] = {
      val expr1_str = buildFilterExpression(expr1).getOrElse("")
      val expr2_str = buildFilterExpression(expr2).getOrElse("")
      Option(s"$expr1_str" + s" $comparisonOp $expr2_str")
    }
    def buildLiteral(value: Any, dataType: DataType): Option[String] = {
      val sqlValue: String = dataType match {
        case StringType => s"""'${value.toString.replace("'", "\\'\\'")}'"""
        case DateType => s"""'${DateFormatter().format(value.asInstanceOf[Int])}'"""
        case TimestampType => s""""${value.asInstanceOf[Timestamp]}""""
        case _ => s"""'${value.toString}'"""
      }
      Option(sqlValue)
    }
    def buildOr(leftFilter: Option[String], rightFilter: Option[String]): Option[String] = {
      val left = leftFilter.getOrElse("")
      val right = rightFilter.getOrElse("")
      Option(s"""( $left OR $right )""")
    }
    def buildAnd(leftFilter: Option[String], rightFilter: Option[String]): Option[String] = {
      val left = leftFilter.getOrElse("")
      val right = rightFilter.getOrElse("")
      Option(s"""( $left AND $right )""")
    }
    def buildNot(filter: Option[String]): Option[String] = {
      val f = filter.getOrElse("")
      Option(s"""NOT ( $f )""")
    }
    def buildAttributeReference(attr: String, exprId: ExprId): Option[String] = {
      if (referenceMap.isEmpty) {
        Option(attr)
      } else {
        // Search the referenceMap for the entry that matches this expression id.
        // Then return back the map key (._1) to generate the prefix to use.
        val referenceName = referenceMap.find(_._2.exists(x => x.exprId == exprId)).get._1
        Option(s"$referenceName.$attr")
      }
    }
    def buildInExpression(value: Expression, list: Seq[Expression]): Option[String] = {
      val arg1 = buildFilterExpression(value).getOrElse("")
      val inStr = s"$arg1 IN ${list.mkString("('", "', '", "')")}"
      Option(inStr)
    }
    def buildInSetExpression(child: Expression, hset: Set[Any]): Option[String] = {
      val arg1 = buildFilterExpression(child).getOrElse("")
      val inStr = s"$arg1 IN ${hset.mkString("(", ",", ")")}"
      Option(inStr)
    }
    def buildMathOp(expr1: Expression,
                    expr2: Expression,
                    mathOp: String): Option[String] = {
      val expr1_str = buildFilterExpression(expr1).getOrElse("")
      val expr2_str = buildFilterExpression(expr2).getOrElse("")
      Option(s"($expr1_str" + s" $mathOp $expr2_str)")
    }

    def buildSubstring(str: Expression,
                       pos: Expression,
                       len: Expression): Option[String] = {
      val str_expr = buildFilterExpression(str).getOrElse("")
      val pos_expr = buildFilterExpression(pos).getOrElse("")
      val len_expr = buildFilterExpression(len).getOrElse("")
      Option(s"substr($str_expr,$pos_expr,$len_expr)")
    }

    filter match {
      case Or(left, right) => buildOr(buildFilterExpression(left),
        buildFilterExpression(right))
      case And(left, right) => buildAnd(buildFilterExpression(left),
        buildFilterExpression(right))
      case Not(filter) => buildNot(buildFilterExpression(filter))
      case EqualTo(attr, value) => buildComparison(attr, value, "=")
      case LessThan(attr, value) => buildComparison(attr, value, "<")
      case GreaterThan(attr, value) => buildComparison(attr, value, ">")
      case LessThanOrEqual(attr, value) => buildComparison(attr, value, "<=")
      case GreaterThanOrEqual(attr, value) => buildComparison(attr, value, ">=")
      // When support is not there, do not push down IS NULL.
      // Allow the pushdown to continue without IS NULL,
      // to help evaluate pushdown.  For production consider to reject
      // the pushdown completely.
      case IsNull(attr) => if (true) {
        Option(s"${attr.asInstanceOf[AttributeReference].name} IS NULL")
        // None // Option("TRUE") // Option(s"${attr.name} IS NULL")
      } else {
        Option("TRUE")
      }
      // When support is not there, do not push down IS NULL.
      // Allow the pushdown to continue without IS NULL,
      // to help evaluate pushdown.  For production consider to reject
      // the pushdown completely.
      case IsNotNull(expr) => if (true) {
        val expr_str = buildFilterExpression(expr).getOrElse("")
        Option(s"$expr_str IS NOT NULL")
        // None // Option("TRUE") // Option(s"${attr.name} IS NOT NULL")
      } else {
        Option("TRUE")
      }
      case StartsWith(attr, value) =>
        val attrStr = buildFilterExpression(attr).getOrElse("")
        Option(s"$attrStr LIKE '$value%'")
      case EndsWith(attr, value) =>
        val attrStr = buildFilterExpression(attr).getOrElse("")
        Option(s"$attrStr LIKE '%$value'")
      case Contains(attr, value) =>
        val attrStr = buildFilterExpression(attr).getOrElse("")
        Option(s"$attrStr LIKE '%$value%'")
      case a@AttributeReference(name, _, _, _) =>
        buildAttributeReference(name, a.exprId)
      case Literal(value, dataType) =>
        buildLiteral(value, dataType)
      case Cast(expression, _, _, _) =>
        buildFilterExpression(expression)
      case In(value, list) =>
        buildInExpression(value, list)
      case InSet(child: Expression, hset: Set[Any]) =>
        buildInSetExpression(child, hset)
      case Add(left, right, _) =>
        buildMathOp(left, right, "+")
      case Subtract(left, right, _) =>
        buildMathOp(left, right, "-")
      case Multiply(left, right, _) =>
        buildMathOp(left, right, "*")
      case Divide(left, right, _) =>
        buildMathOp(left, right, "/")
      case Substring(str, pos, len) =>
        buildSubstring(str, pos, len)
      case other@_ => logger.info("unknown filter:" + other) ; None
    }
  }

  /** Returns a string to represent the input query.
   *
   * @return String representing the query to send to the endpoint.
   */
  def query: String = {
    val columnList = {
      if (schema.length != 0) {
        schema.fields.map(x => s"" + s"${x.name}").mkString(",")
      } else {
        // There is no schema, just return all columns.
        "*"
      }
    }
    val whereClause = buildWhereClause()
    val objectClause = "TABLE_TAG"
    var retVal = ""
    val groupByClause = "" // getGroupByClause(aggregation)
    if (whereClause.isEmpty) {
      retVal = s"SELECT $columnList FROM $objectClause $groupByClause"
    } else {
      retVal = s"SELECT $columnList FROM $objectClause $whereClause $groupByClause"
    }
    retVal
  }
  def columns: JsonArrayBuilder = {
    val arrayBuilder = Json.createArrayBuilder()
    queryCols.foreach(c => arrayBuilder.add(c))
    arrayBuilder
  }
  def jsonQuery: String = {
    val projectionNodeBuilder = Json.createObjectBuilder()
    projectionNodeBuilder.add("Name", "SQL")
    projectionNodeBuilder.add("Type", "_SQL")
    projectionNodeBuilder.add("Query", this.query)
    projectionNodeBuilder.add("Columns", this.columns)

    val stringWriter = new StringWriter()
    val writer = Json.createWriter(stringWriter)
    writer.writeObject(projectionNodeBuilder.build())
    writer.close()
    val jsonString = stringWriter.getBuffer.toString
    jsonString
  }
//  private def getGroupByClause(aggregation: Option[AggregateExpression]): String = {
//    if ((aggregation != None) &&
//      (aggregation.get.groupByColumns.length > 0)) {
//      val quotedColumns =
//        aggregation.get.groupByColumns.map(c => s"${getColString(c.fieldNames.head)}")
//      s"GROUP BY ${quotedColumns.mkString(", ")}"
//    } else {
//      ""
//    }
//  }
}

object PushdownSQL {

  protected val logger: Logger = LoggerFactory.getLogger(getClass)
  def apply(schema: StructType,
            filters: Seq[Expression],
            queryCols: Array[String],
            referenceMap: Map[String, Seq[AttributeReference]] =
           Map[String, Seq[AttributeReference]]()): PushdownSQL = {
    new PushdownSQL(schema, filters, queryCols, referenceMap)
  }

  private val filterMaxDepth = 100

  def checkHandleFilterExpression(expr: Expression, depth: Int = 0): Boolean = {
    if (depth > filterMaxDepth) {
      /* Reached depth unsupported by NDP server. */
      return false
    }
    /* Traverse the tree and validate the nodes are supported. */
    expr match {
      case Or(left, right) => checkHandleFilterExpression(left, depth + 1) &&
        checkHandleFilterExpression(right, depth + 1)
      case And(left, right) => checkHandleFilterExpression(left, depth + 1) &&
                              checkHandleFilterExpression(right, depth + 1)
      case Not(filter) => checkHandleFilterExpression(filter, depth + 1)
      case EqualTo(left, right) => checkHandleFilterExpression(left, depth + 1) &&
        checkHandleFilterExpression(right, depth + 1)
      case LessThan(left, right) => checkHandleFilterExpression(left, depth + 1) &&
        checkHandleFilterExpression(right, depth + 1)
      case GreaterThan(left, right) => checkHandleFilterExpression(left, depth + 1) &&
        checkHandleFilterExpression(right, depth + 1)
      case LessThanOrEqual(left, right) => checkHandleFilterExpression(left, depth + 1) &&
        checkHandleFilterExpression(right, depth + 1)
      case GreaterThanOrEqual(left, right) => checkHandleFilterExpression(left, depth + 1) &&
        checkHandleFilterExpression(right, depth + 1)
      case IsNull(attr) => checkHandleFilterExpression(attr, depth + 1)
      case IsNotNull(attr) => checkHandleFilterExpression(attr, depth + 1)
      case StartsWith(left, right) => checkHandleFilterExpression(left, depth + 1) &&
                                    checkHandleFilterExpression(right, depth + 1)
      case EndsWith(left, right) => checkHandleFilterExpression(left, depth + 1) &&
                                    checkHandleFilterExpression(right, depth + 1)
      case Contains(left, right) => checkHandleFilterExpression(left, depth + 1) &&
                                    checkHandleFilterExpression(right, depth + 1)
      case _: AttributeReference =>
        true
      case _: Literal =>
        true
      case _: Cast =>
        true
      case _: In =>
        true
      case _: InSet =>
        true
      case _: ScalarSubquery =>
        false
      case _: Divide =>
        true
      case _: Substring =>
        true
      case other@_ => logger.warn("unknown checkHandle filter:" + other)
        true
    }
  }
  def canHandleFilters(filters: Seq[Expression]): Boolean = {
    var invalidCount = 0
    var validCount = 0
    for (f <- filters) {
      if (checkHandleFilterExpression(f)) {
        validCount += 1
      } else {
        invalidCount += 1
      }
    }
    if (invalidCount > 0) {
      false
    } else {
      true
    }
  }
  def validateFilterExpression(expr: Expression, depth: Int = 0): Boolean = {
    if (depth > filterMaxDepth) {
      /* Reached depth unsupported by NDP server. */
      return false
    }
    /* Traverse the tree and validate the nodes are supported. */
    expr match {
      case Or(left, right) => validateFilterExpression(left, depth + 1) &&
        validateFilterExpression(right, depth + 1)
      case And(left, right) => validateFilterExpression(left, depth + 1) &&
                              validateFilterExpression(right, depth + 1)
      case Not(filter) => validateFilterExpression(filter, depth + 1)
      case EqualTo(left, right) => validateFilterExpression(left, depth + 1) &&
        validateFilterExpression(right, depth + 1)
      case LessThan(left, right) => validateFilterExpression(left, depth + 1) &&
        validateFilterExpression(right, depth + 1)
      case GreaterThan(left, right) => validateFilterExpression(left, depth + 1) &&
        validateFilterExpression(right, depth + 1)
      case LessThanOrEqual(left, right) => validateFilterExpression(left, depth + 1) &&
        validateFilterExpression(right, depth + 1)
      case GreaterThanOrEqual(left, right) => validateFilterExpression(left, depth + 1) &&
        validateFilterExpression(right, depth + 1)
      case IsNull(attr) => validateFilterExpression(attr, depth + 1)
      case IsNotNull(attr) => validateFilterExpression(attr, depth + 1)
      case StartsWith(left, right) => validateFilterExpression(left, depth + 1) &&
                                    validateFilterExpression(right, depth + 1)
      case EndsWith(left, right) => validateFilterExpression(left, depth + 1) &&
                                    validateFilterExpression(right, depth + 1)
      case Contains(left, right) => validateFilterExpression(left, depth + 1) &&
                                    validateFilterExpression(right, depth + 1)
      case _: AttributeReference =>
        true
      case _: Literal =>
        true
      case _: Cast =>
        true
      case _: In =>
        true
      case _: InSet =>
        true
      case _: Divide =>
        true
      case _: Substring =>
        true
      case other@_ => logger.warn("unknown filter:" + other)
        /* Reached an unknown node, return validation failed. */
        false
    }
  }
  def validateFilters(filters: Seq[Expression]): PushdownSqlStatus = {
    var invalidCount = 0
    var validCount = 0
    for (f <- filters) {
      if (validateFilterExpression(f)) {
        validCount += 1
      } else {
        invalidCount += 1
      }
    }
    if (invalidCount == 0 && validCount > 0) {
      PushdownSqlStatus.FullyValid
    } else if (invalidCount > 0 && validCount > 0) {
      PushdownSqlStatus.PartiallyValid
    } else if (filters.isEmpty) {
      PushdownSqlStatus.FullyValid
    } else {
      PushdownSqlStatus.Invalid
    }
  }

  def getAggregateSchema(aggregates: Seq[AggregateExpression],
                         groupingExpressions: Seq[Expression]): StructType = {
    var schema = new StructType()
    for (e <- groupingExpressions) {
      e match {
        case AttributeReference(name, dataType, _, _) =>
          schema = schema.add(StructField(name, dataType))
      }
    }
    for (a <- aggregates) {
      a.aggregateFunction match {
        case min @ Min(PushableColumnWithoutNestedColumn(_)) =>
          schema = schema.add(StructField(s"min(name)", min.dataType))
        case max @ Max(PushableColumnWithoutNestedColumn(_)) =>
          schema = schema.add(StructField(s"max(name)", max.dataType))
        case count: aggregate.Count if count.children.length == 1 =>
          count.children.head match {
            // SELECT COUNT(*) FROM table is translated to SELECT 1 FROM table
            case Literal(_, _) =>
              schema = schema.add(StructField("count(*)", LongType))
            case PushableColumnWithoutNestedColumn(name) =>
              schema = schema.add(StructField(s"count($name)", LongType))
          }
        case sum @ Sum(PushableColumnWithoutNestedColumn(name), _) =>
          schema = schema.add(StructField(s"sum($name)", sum.dataType))
        case sum @ Sum(child: Expression, _) =>
          schema = schema.add(StructField(s"sum(${getAggregateString(child)})", sum.dataType))
      }
    }
    schema
  }

  def getAggregateString(e: Expression): String = {
    e match {
      case AttributeReference(name, _, _, _) =>
        name
      case Literal(value, _) =>
        value.toString
      case Multiply(left, right, _) =>
        s"(${getAggregateString(left)} * ${getAggregateString(right)})"
      case Divide(left, right, _) =>
        s"(${getAggregateString(left)} / ${getAggregateString(right)})"
      case Add(left, right, _) =>
        s"(${getAggregateString(left)} + ${getAggregateString(right)})"
      case Subtract(left, right, _) =>
        s"(${getAggregateString(left)} - ${getAggregateString(right)})"
    }
  }
  def buildAggregateExpression(aggregate: AggregateExpression): Option[String] = {
    def buildAggComp(left: Expression, right: Expression, comparisonOp: String): String = {
      s"${buildAggExpr(left)} $comparisonOp ${buildAggExpr(right)}"
    }
    def buildAggExpr(e: Expression) : String = {
      e match {
        case AttributeReference(name, _, _, _) =>
          name
        case Literal(value, _) =>
          value.toString
        case Multiply(left, right, _) =>
          buildAggComp(left, right, "*")
        case Divide(left, right, _) =>
          buildAggComp(left, right, "/")
        case Add(left, right, _) =>
          buildAggComp(left, right, "+")
        case Subtract(left, right, _) =>
          buildAggComp(left, right, "-")
      }
    }
    def buildCount(value: Any,
                   isDistinct: Boolean = false): String = {
      if (isDistinct) {
        s"COUNT(DISTINCT $value)"
      } else {
        s"COUNT($value)"
      }
    }
    if (aggregate.filter.isEmpty) {
      aggregate.aggregateFunction match {
        case Min(PushableColumnWithoutNestedColumn(name)) =>
          Some("MIN(" + name + ")")
        case Max(PushableColumnWithoutNestedColumn(name)) =>
          Some("MAX(" + name + ")")
        case count: Count if count.children.length == 1 =>
          count.children.head match {
            // SELECT COUNT(*) FROM table is translated to SELECT 1 FROM table
            case Literal(_, _) =>
              Some(buildCount("*",
                              isDistinct = aggregate.isDistinct))
            case PushableColumnWithoutNestedColumn(name) =>
              Some(buildCount(name,
                              isDistinct = aggregate.isDistinct))
            case _ => None
          }
        case Sum(PushableColumnWithoutNestedColumn(name), _) =>
          Some("SUM(" + name + ")")
        case Sum(child: Expression, _) =>
          Some("SUM(" + buildAggExpr(child) + ")")
        case _ => None
      }
    } else {
      None
    }
  }
  def getAggregateExpressionSql(aggregateExpressions: Seq[AggregateExpression]): String = {
    var sql: String = ""
    for (f <- aggregateExpressions) {
      val a = buildAggregateExpression(f)
      if (a.isDefined) {
        // If we decide to save as parquet, we need the column names to not contain
        // any invalid characters.
        val regex = "[()*]".r
        val aliasStr = if (regex.findFirstMatchIn(a.get).isDefined) {
          s"${a.get} as ${a.get.replaceAll("[)(*]", "_")}"
        } else {
          a.get
        }
        if (sql != "") {
          sql += "," + aliasStr
        } else {
          sql += aliasStr
        }
      }
    }
    sql
  }
  def getGroupbyExpression(groupingExpressions: Seq[Expression]): String = {
    var groupbySql = ""
    for (e <- groupingExpressions) {
      val expr = {
        e match {
          case PushableColumnWithoutNestedColumn(name) =>
            Some(name)
        }
      }
      groupbySql += s"${expr.get} "
    }
    groupbySql
  }
  def getAggregateSql(aggregateExpressions: Seq[AggregateExpression],
                      groupingExpressions: Seq[Expression],
                      query: String): String = {
    val aggregateSql = getAggregateExpressionSql(aggregateExpressions)
    if (groupingExpressions.isEmpty) {
      s"SELECT $aggregateSql FROM $query"
    } else {
      val groupby = getGroupbyExpression(groupingExpressions)
      s"SELECT $groupby,$aggregateSql FROM $query GROUP BY $groupby"
    }
  }
}

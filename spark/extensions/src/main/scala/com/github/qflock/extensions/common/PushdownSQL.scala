// scalastyle:off
/*
 * Copyright 2018 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Note that portions of this code came from spark-select code:
 *  https://github.com/minio/spark-select/blob/master/src/main/scala/io/minio/spark/select/FilterPushdown.scala
 *
 * Other portions of this code, most notably compileAggregates, and getColumnSchema,
 * came from this patch by Huaxin Gao:
 *   https://github.com/apache/spark/pull/29695
 */
// scalastyle:on
package com.github.qflock.extensions.common

import java.io.StringWriter
import java.sql.{Date, Timestamp}
import javax.json.Json
import javax.json.JsonArrayBuilder

import com.github.qflock.extensions.common.PushdownSqlStatus.PushdownSqlStatus
import org.slf4j.LoggerFactory

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count, Max, Min, Sum}
import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.types._


/** Provides pushdown capabilities aimed at
 *  generating information needed for pushdown
 *  from the inputs that Spark provides.
 */
class PushdownSQL(schema: StructType,
                  filters: Seq[Expression],
                  queryCols: Array[String]) {

  protected val logger = LoggerFactory.getLogger(getClass)
  protected val validFilters = filters.filter(f => PushdownSQL.validateFilterExpression(f))

  /**
   * Use the given schema to look up the attribute's data type. Returns None if the attribute could
   * not be resolved.
   */
  private def getTypeForAttribute(attribute: String): Option[DataType] = {
    if (schema.fieldNames.contains(attribute)) {
      Some(schema(attribute).dataType)
    } else {
      None
    }
  }
  /**
   * Build a SQL WHERE clause for the given filters. If a filter cannot be pushed down then no
   * condition will be added to the WHERE clause. If none of the filters can be pushed down then
   * an empty string will be returned.
   *
   * @param schema the schema of the table being queried
   * @param filters an array of filters, the conjunction of which is the filter condition for the
   *                scan.
   */
  def buildWhereClause(): String = {
    val filterExpressions = validFilters.flatMap(f => buildFilterExpression(f)).mkString(" AND ")
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
      Option(s"${expr1_str}" + s" $comparisonOp ${expr2_str}")
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
    def buildAttributeReference(attr: String): Option[String] = Option(attr)
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
      Option(s"(${expr1_str}" + s" $mathOp ${expr2_str})")
    }

    def buildSubstring(str: Expression,
                       pos: Expression,
                       len: Expression): Option[String] = {
      val str_expr = buildFilterExpression(str).getOrElse("")
      val pos_expr = buildFilterExpression(pos).getOrElse("")
      val len_expr = buildFilterExpression(len).getOrElse("")
      Option(s"substr(${str_expr},${pos_expr},${len_expr})")
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
        Option(s"${expr_str} IS NOT NULL")
        // None // Option("TRUE") // Option(s"${attr.name} IS NOT NULL")
      } else {
        Option("TRUE")
      }
      case StartsWith(attr, value) =>
        val attrStr = buildFilterExpression(attr).getOrElse("")
        Option(s"${attrStr} LIKE '${value}%'")
      case EndsWith(attr, value) =>
        val attrStr = buildFilterExpression(attr).getOrElse("")
        Option(s"${attrStr} LIKE '%${value}'")
      case Contains(attr, value) =>
        val attrStr = buildFilterExpression(attr).getOrElse("")
        Option(s"${attrStr} LIKE '%${value}%'")
      case AttributeReference(name, dataType, nullable, meta) =>
        buildAttributeReference(name)
      case Literal(value, dataType) =>
        buildLiteral(value, dataType)
      case Cast(expression, dataType, timeZoneId, _) =>
        buildFilterExpression(expression)
      case in@In(value, list) =>
        buildInExpression(value, list)
      case in@InSet(child: Expression, hset: Set[Any]) =>
        buildInSetExpression(child, hset)
      case Add(left, right, failOnError) =>
        buildMathOp(left, right, "+")
      case Subtract(left, right, failOnError) =>
        buildMathOp(left, right, "-")
      case Multiply(left, right, failOnError) =>
        buildMathOp(left, right, "*")
      case Divide(left, right, failOnError) =>
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
    var columnList = {
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
    if (whereClause.length == 0) {
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
    val jsonString = stringWriter.getBuffer().toString()
    // val indented = (new JSONObject(jsonString)).toString(4)
    jsonString
  }
}

object PushdownSQL {

  protected val logger = LoggerFactory.getLogger(getClass)
  def apply(schema: StructType,
            filters: Seq[Expression],
            queryCols: Array[String]): PushdownSQL = {
    new PushdownSQL(schema, filters, queryCols)
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
      case attrib @ AttributeReference(name, dataType, nullable, meta) =>
        true
      case Literal(value, dataType) =>
        true
      case Cast(expression, dataType, timeZoneId, _) =>
        true
      case In(value, list) =>
        true
      case InSet(child, hset) =>
        true
      case ScalarSubquery(plan, outerAttrs, exprId, joinCond) =>
        false
      case Divide(left, right, failOnError) =>
        true
      case Substring(str, pos, len) =>
        true
      case other@_ => logger.warn("unknown checkHandle filter:" + other)
        true
    }
  }
  def canHandleFilters(filters: Seq[Expression]): Boolean = {
    var status: Boolean = true
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
      case attrib @ AttributeReference(name, dataType, nullable, meta) =>
        true
      case Literal(value, dataType) =>
        true
      case Cast(expression, dataType, timeZoneId, _) =>
        true
      case In(value, list) =>
        true
      case InSet(child, hset) =>
        true
      case Divide(left, right, failOnError) =>
        true
      case Substring(str, pos, len) =>
        true
      case other@_ => logger.warn("unknown filter:" + other)
        /* Reached an unknown node, return validation failed. */
        false
    }
  }
  def validateFilters(filters: Seq[Expression]): PushdownSqlStatus = {
    var status: Boolean = true
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
    } else if (filters.length == 0) {
      PushdownSqlStatus.FullyValid
    } else {
      PushdownSqlStatus.Invalid
    }
  }
}
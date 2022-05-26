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

import java.sql.ResultSet

import com.github.qflock.jdbc.QflockResultSet

import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.unsafe.types.UTF8String

/** Represents a ColumnVector to wrap a column from a jdbc ResultSet.
 *  @param dataType the Int representing the NdpDataType.
 *  @param schema the schema returned from the server.
 *  @return
 */
class QflockJdbcColumnVector(dataType: DataType, schema: StructType)
  extends ColumnVector(schema: StructType) {

  val typeSize: Int = {
    dataType match {
      case StringType => 8
      case IntegerType => 4
      case DoubleType => 8
      case LongType => 8
    }
  }
  var resultSet: Option[QflockResultSet] = None
  var columnIndex: Option[Int] = None

  def close(): Unit = {}
  def getArray(row: Int): org.apache.spark.sql.vectorized.ColumnarArray = { null }
  def getBinary(row: Int): Array[Byte] = { null }
  def getBoolean(row: Int): Boolean = { false }
  def getByte(row: Int): Byte = { resultSet.get.getByte(columnIndex.get, row) }
  def getChild(row: Int): org.apache.spark.sql.vectorized.ColumnVector = { null }
  def getDecimal(row: Int, r: Int, p: Int): org.apache.spark.sql.types.Decimal = { Decimal(0) }
  def getDouble(row: Int): Double = {
    resultSet.get.getDouble(columnIndex.get + 1, row + 1)
  }
  def getFloat(row: Int): Float = {
    resultSet.get.getFloat(columnIndex.get + 1, row + 1)
  }
  def getInt(row: Int): Int = {
    resultSet.get.getInt(columnIndex.get + 1, row + 1)
  }
  def getLong(row: Int): Long = {
    resultSet.get.getLong(columnIndex.get + 1, row + 1)
  }
  def getMap(row: Int): org.apache.spark.sql.vectorized.ColumnarMap = { null }
  def getShort(row: Int): Short = { resultSet.get.getShort(columnIndex.get, row) }
  def getUTF8String(row: Int): org.apache.spark.unsafe.types.UTF8String = {
    UTF8String.fromString(resultSet.get.getString(columnIndex.get + 1, row + 1))
  }
  def hasNull: Boolean = { false }
  def isNullAt(row: Int): Boolean = { false }
  def numNulls(): Int = { 0 }

  /** Initializes this column from a resultSet.
   *
   *  @return Int the number of rows returned.
   */
  def setupColumn(colIdx: Integer, resSet: ResultSet): Int = {
    columnIndex = Some(colIdx)
    resultSet = Some(resSet.asInstanceOf[QflockResultSet])
    resultSet.get.getNumRows
  }
}

object QflockJdbcColumnVector {

  /** Returns an array of QflockJdbcColumnVector.
   *  Use of an NdpColumnVector is always in sets to represent
   *  batches of data.  Thus they are only useful in sets.
   *  This provides the api to return a relevant set of
   *  NdpColumnVectors representing the appropriate types.
   *
   *  @param schema the relevant schema for the vector.
   */
  def apply(schema: StructType): Array[QflockJdbcColumnVector] = {
    val vectors = new Array[QflockJdbcColumnVector](schema.fields.length)
    for (i <- schema.fields.indices) {
      vectors(i) = new QflockJdbcColumnVector(schema.fields(i).dataType, schema)
    }
    vectors
  }
}

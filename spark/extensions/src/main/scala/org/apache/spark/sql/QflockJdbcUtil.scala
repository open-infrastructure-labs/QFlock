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
package org.apache.spark.sql


import java.sql.ResultSet

import com.github.qflock.extensions.QflockJdbcDialect

import org.apache.spark.TaskContext
import org.apache.spark.executor.InputMetrics
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils


object QflockJdbcUtil {

  def getResultSetRowIterator(resultSet: ResultSet): Iterator[InternalRow] = {
    val inputMetrics =
      Option(TaskContext.get()).map(_.taskMetrics().inputMetrics).getOrElse(new InputMetrics)
    val dialect = new QflockJdbcDialect()
    val schema = JdbcUtils.getSchema(resultSet, dialect)
    val rowsIterator = JdbcUtils.resultSetToSparkInternalRows(resultSet, schema, inputMetrics)

//    CompletionIterator[InternalRow, Iterator[InternalRow]](
//      new InterruptibleIterator(context, rowsIterator), close())
    rowsIterator
  }
}

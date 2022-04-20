#!/usr/bin/python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import inspect
import logging
import pyspark
from pyspark.sql.types import StringType, DoubleType, IntegerType, LongType, ShortType
import numpy as np

from com.github.qflock.jdbc.api import QflockJdbcService
from com.github.qflock.jdbc.api import ttypes


class QflockThriftJdbcHandler:
    def __init__(self, spark_log_level="INFO"):
        self._connections = {}
        self._pstatements = {}
        self._connection_id = 0
        self._pstatement_id = 0
        self._query_id = 0
        self._spark = pyspark.sql.SparkSession \
            .builder \
            .appName("qflock-jdbc") \
            .enableHiveSupport() \
            .getOrCreate()
        self._spark.sparkContext.setLogLevel(spark_log_level)

    def createConnection(self, url, properties):
        current_id = self._connection_id
        dbname = url.split(";")[0].lstrip("/")
        logging.info(f"New connection id {current_id} dbname {dbname} url {url} properties {str(properties)}")
        self._connection_id += 1
        self._connections[current_id] = {'url': url, 'properties': properties,
                                         'dbname': dbname}
        return ttypes.QFConnection(id=current_id)

    def createStatement(self, connection):
        logging.info(f"createStatement connection id: {connection.id}")
        return ttypes.QFStatement(id=42, sql=None, id_connection=connection.id)

    def createPreparedStatement(self, connection):
        current_id = self._pstatement_id
        logging.info(f"createStatement id {current_id} connection id: {connection.id}")
        self._pstatement_id += 1
        self._pstatements[current_id] = {'connection': connection}
        return ttypes.QFStatement(id=current_id, sql=None, id_connection=connection.id)

    def connection_getstaticmetadata(self, connection):
        """
        Parameters:
         - connection

        """
        logging.debug("connection_getstaticmetadata")

    def connection_isvalid(self, connection, timeout):
        """
        Parameters:
         - connection
         - timeout

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def connection_setAutoCommit(self, connection, autoCommit):
        """
        Parameters:
         - connection
         - autoCommit

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def connection_getAutoCommit(self, connection):
        """
        Parameters:
         - connection

        """
        logging.debug("connection_getAutoCommit::")
        return QflockJdbcService.connection_getAutoCommit_result(success=True)

    def connection_setTransactionIsolation(self, connection, level):
        """
        Parameters:
         - connection
         - level

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def connection_getTransactionIsolation(self, connection):
        """
        Parameters:
         - connection

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def connection_setReadOnly(self, connection, readOnly):
        """
        Parameters:
         - connection
         - readOnly

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def connection_getReadOnly(self, connection):
        """
        Parameters:
         - connection

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def connection_setCatalog(self, connection, catalog):
        """
        Parameters:
         - connection
         - catalog

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def connection_getCatalog(self, connection):
        """
        Parameters:
         - connection

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def connection_setSchema(self, connection, schema):
        """
        Parameters:
         - connection
         - schema

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def connection_getSchema(self, connection):
        """
        Parameters:
         - connection

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def connection_getCatalogSeparator(self, connection):
        """
        Parameters:
         - connection

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def connection_getCatalogTerm(self, connection):
        """
        Parameters:
         - connection

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def connection_getSchemaTerm(self, connection):
        """
        Parameters:
         - connection

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def connection_getCatalogs(self, connection):
        """
        Parameters:
         - connection

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def connection_getSchemas(self, connection, catalog, schemaPattern):
        """
        Parameters:
         - connection
         - catalog
         - schemaPattern

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def connection_getTables(self, connection, catalog, schemaPattern, tableNamePattern, types):
        """
        Parameters:
         - connection
         - catalog
         - schemaPattern
         - tableNamePattern
         - types

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def connection_getColumns(self, connection, catalog, schemaPattern, tableNamePattern, columnNamePattern):
        """
        Parameters:
         - connection
         - catalog
         - schemaPattern
         - tableNamePattern
         - columnNamePattern

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def connection_getSQLKeywords(self, connection):
        """
        Parameters:
         - connection

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def connection_getTableTypes(self, connection):
        """
        Parameters:
         - connection

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def connection_getTypeInfo(self, connection):
        """
        Parameters:
         - connection

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def closeConnection(self, connection):
        """
        Parameters:
         - connection

        """
        if connection.id in self._connections:
            del self._connections[connection.id]
            logging.info(f"successfully closed connection {connection.id}")
        else:
            logging.warning(f"connection id {connection.id} not found")

    def statement_close(self, statement):
        """
        Parameters:
         - statement

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def statement_execute(self, statement, sql):
        """
        Parameters:
         - statement
         - sql

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def statement_executeQuery(self, statement, sql):
        """
        Parameters:
         - statement
         - sql

        """
        logging.debug(f"statement_executeQuery:: statement id: {statement.id} sql: {sql}")
        row = ttypes.QFRow([ttypes.QFValue(isnull=False, val=ttypes.RawVal(integer_val=42))])
        rows = [row]
        parts = [ttypes.QFResultSetMetaDataPart(columnName='fakecol1', columnType='INT64')]
        metadata = ttypes.QFResultSetMetaData(parts)
        return ttypes.QFResultSet(42, rows, metadata)

    def map_data_type(self, data_type):
        if isinstance(data_type, StringType):
            # java.sql.Types.VARCHAR (JdbcUtil.getSchema
            return (12, False)
        elif isinstance(data_type, DoubleType):
            # java.sql.Types.DOUBLE
            return (8, False)
        elif isinstance(data_type, LongType):
            # java.sql.Types.INTEGER
            return (4, False)
        elif isinstance(data_type, IntegerType):
            # java.sql.Types.INTEGER
            return (4, False)
        else:
            print("unknown type")
            raise Exception(f"unkown type {data_type}")

    def get_metadata(self, df_schema):
        parts = []
        for field in df_schema.fields:
            (columnType, signed) = self.map_data_type(field.dataType)
            parts.append(ttypes.QFResultSetMetaDataPart(columnName=field.name,
                                                        columnType=columnType,
                                                        columnLabel=field.name,
                                                        signed=signed))
        metadata = ttypes.QFResultSetMetaData(parts)
        return metadata

    def map_value(self, data_type, value):
        if isinstance(data_type, StringType):
            return ttypes.RawVal(string_val=value)
        elif isinstance(data_type, DoubleType):
            return ttypes.RawVal(double_val=value)
        elif isinstance(data_type, LongType):
            return ttypes.RawVal(integer_val=value)
        elif isinstance(data_type, IntegerType):
            return ttypes.RawVal(integer_val=value)
        else:
            print("unknown type")
            raise Exception(f"unknown type {data_type}")

    @classmethod
    def data_type_size(cls, data_type):
        if isinstance(data_type, DoubleType):
            return 8
        elif isinstance(data_type, LongType):
            return 8
        elif isinstance(data_type, IntegerType):
            return 4
        elif isinstance(data_type, ShortType):
            return 2
        else:
            # String also falls in this case.  We need to determine size per string.
            raise Exception(f"unknown type {data_type}")

    def get_query_id(self):
        query_id = self._query_id
        self._query_id += 1
        return query_id

    def exec_query(self, sql):
        query_id = self.get_query_id()
        query = sql.replace('\"', "") #+ " LIMIT 10"
        logging.info(f"query id: {query_id} starting {query}")
        df = self._spark.sql(query)
        df_pandas = df.toPandas()
        num_rows = len(df_pandas.index)
        logging.info(f"query toPandas() done {query} rows {num_rows}")
        df_schema = df.schema
        binary_rows = []
        col_size = []
        if num_rows > 0:
            columns = df.columns
            for col_idx in range(0, len(columns)):
                # data = np_array[:,col_idx]
                data = df_pandas[columns[col_idx]].to_numpy()
                data_type = df_schema.fields[col_idx].dataType
                if isinstance(data_type, StringType):
                    new_data1 = data.astype(str)
                    new_data = np.char.encode(new_data1, encoding='utf-8')
                    logging.debug(f"item size for col idx: {col_idx} is: {new_data.dtype.itemsize}")
                    binary_rows.append(new_data.tobytes())
                    col_size.append(new_data.dtype.itemsize)
                else:
                    new_data = data.byteswap().newbyteorder().tobytes()
                    binary_rows.append(new_data)
                    col_size.append(QflockThriftJdbcHandler.data_type_size(data_type))

        logging.info(f"query done {query} rows {num_rows}")
        return ttypes.QFResultSet(id=query_id, metadata=self.get_metadata(df_schema),
                                  numRows=num_rows, binaryRows=binary_rows, columnSize=col_size)

    def statement_getResultSet(self, statement):
        """
        Parameters:
         - statement

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def statement_getUpdateCount(self, statement):
        """
        Parameters:
         - statement

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def statement_getResultSetType(self, statement):
        """
        Parameters:
         - statement

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def statement_cancel(self, statement):
        """
        Parameters:
         - statement

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def statement_getWarnings(self, statement):
        """
        Parameters:
         - statement

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def statement_clearWarnings(self, statement):
        """
        Parameters:
         - statement

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def statement_getMaxRows(self, statement):
        """
        Parameters:
         - statement

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def statement_setMaxRows(self, statement, max):
        """
        Parameters:
         - statement
         - max

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def statement_getQueryTimeout(self, statement):
        """
        Parameters:
         - statement

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def statement_setQueryTimeout(self, statement, seconds):
        """
        Parameters:
         - statement
         - seconds

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def preparedStatement_close(self, statement):
        """
        Parameters:
         - statement

        """
        if statement.id in self._pstatements:
            del self._pstatements[statement.id]
            logging.info(f"successfully closed preparedStatement {statement.id}")
        else:
            logging.warning(f"preparedStatement id {statement.id} not found")

    def preparedStatement_execute(self, statement, sql):
        """
        Parameters:
         - statement
         - sql

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def preparedStatement_executeQuery(self, statement, sql):
        """
        Parameters:
         - statement
         - sql

        """
        connection_id = self._pstatements[statement.id]['connection'].id
        connection = self._connections[connection_id]
        logging.info(f"preparedStatement_executeQuery:: statement id: {statement.id} sql: {sql}" +\
                     f" db {connection['dbname']}")
        # row = ttypes.QFRow([ttypes.QFValue(isnull=False, val=ttypes.RawVal(integer_val=42))])
        # rows = [row]
        # # 4 = java.sql.Types.INTEGER (JdbcUtil.getSchema
        # columnName = 'fakecol1'
        # parts = [ttypes.QFResultSetMetaDataPart(columnName=columnName, columnType=4,
        #                                         columnLabel=columnName, signed=False)]
        # metadata = ttypes.QFResultSetMetaData(parts)
        # return ttypes.QFResultSet(42, rows, metadata)
        self._spark.sql(f"USE {connection['dbname']}")
        return self.exec_query(sql)

    def preparedStatement_getResultSet(self, statement):
        """
        Parameters:
         - statement

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def preparedStatement_getUpdateCount(self, statement):
        """
        Parameters:
         - statement

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def preparedStatement_getResultSetType(self, statement):
        """
        Parameters:
         - statement

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def preparedStatement_cancel(self, statement):
        """
        Parameters:
         - statement

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def preparedStatement_getWarnings(self, statement):
        """
        Parameters:
         - statement

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def preparedStatement_clearWarnings(self, statement):
        """
        Parameters:
         - statement

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def preparedStatement_getMaxRows(self, statement):
        """
        Parameters:
         - statement

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def preparedStatement_setMaxRows(self, statement, max):
        """
        Parameters:
         - statement
         - max

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def preparedStatement_getQueryTimeout(self, statement):
        """
        Parameters:
         - statement

        """
        logging.debug(inspect.currentframe().f_code.co_name)

    def preparedStatement_setQueryTimeout(self, statement, seconds):
        """
        Parameters:
         - statement
         - seconds

        """
        logging.debug(inspect.currentframe().f_code.co_name)







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
import os
import threading
import inspect
import logging
import pyspark
from pyspark.sql.types import StringType, DoubleType, IntegerType, LongType, ShortType
import numpy as np

import pyarrow
import pyarrow.parquet
import zstandard as zstd

from com.github.qflock.jdbc.api import QflockJdbcService
from com.github.qflock.jdbc.api import ttypes

from metastore_client import HiveMetastoreClient
from py4j.java_gateway import java_import


class QflockThriftJdbcHandler:
    def __init__(self, spark_log_level="INFO",
                 metastore_ip="", metastore_port="", debug_pyspark=False,
                 max_views=4, compression=True):
        self._max_views = max_views
        self._compression = compression
        self._lock = threading.Lock()
        self._connections = {}
        self._pstatements = {}
        self._connection_id = 0
        self._pstatement_id = 0
        self._query_id = 0
        if debug_pyspark:
            self._create_debug_spark()
        else:
            self._create_spark()
        self._spark.sparkContext.setLogLevel(spark_log_level)
        self._metastore_client = HiveMetastoreClient(metastore_ip, metastore_port)
        self._get_tables()
        self._cached_views = {}
        self._table_paths = {}
        self._ds_table_desc = {}
        self._gw = self._spark.sparkContext._gateway
        java_import(self._gw.jvm, "com.github.qflock.datasource.QflockTableDescriptor")
        self._create_views()
        logging.info(f"initialized compression: {compression}")

    def _create_debug_spark(self):
        self._spark = pyspark.sql.SparkSession \
            .builder \
            .appName("qflock-jdbc")\
            .config("spark.driver.maxResultSize", "10g")\
            .config("spark.driver.memory", "20g")\
            .config("spark.executor.memory", "20g")\
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")\
            .config("spark.sql.catalogImplementation", "hive")\
            .config("spark.sql.warehouse.dir", "hdfs://qflock-storage-dc1:9000/user/hive/warehouse3")\
            .config("spark.hadoop.hive.metastore.uris", "thrift://qflock-storage-dc1:9084")\
            .config("spark.jars", "../../spark/extensions/target/scala-2.12/qflock-extensions_2.12-0.1.0.jar")\
            .getOrCreate()

    def _create_spark(self):
        # .enableHiveSupport()\
        # We do not use hive since we create
        # our own temp views with names of the table.
        self._spark = pyspark.sql.SparkSession \
            .builder \
            .appName("qflock-jdbc")\
            .getOrCreate()

    def _get_tables(self):
        client = self._metastore_client.client
        dbs = self._metastore_client.client.get_all_databases()
        tables = []
        logging.info(f"Fetching tables from metastore")
        for db_name in dbs:
            table_names = client.get_all_tables(db_name)
            tables.extend([client.get_table(db_name, table_name)for table_name in table_names])
        self._tables = tables
        logging.info(f"Fetching tables from metastore...Complete")

    def _create_view(self, table, request_id, schema, file_path):
        # Each table view has a request_id to identify it.
        # The request ID will chosen by a call to
        # the table descriptor's fillRequestInfo the by the client
        #
        logging.info(f"Create view for table: {table.tableName} request_id: {request_id}")
        df = self._spark.read \
            .format("qflockDs") \
            .option("format", "parquet") \
            .option("schema", schema)\
            .option("tableName", table.tableName) \
            .option("path", file_path)\
            .option("dbName", table.dbName) \
            .option("requestId", request_id) \
            .load()
        view_name = f"{table.tableName}_{request_id}"
        df.createOrReplaceTempView(view_name)
        self._cached_views[view_name] = {'name': table.tableName, 'request_id': request_id,
                                         'dataframe': df}

    def _get_schema(self, table):
        def convert_col(type):
            if type == "bigint":
                return "long"
            elif type == "double":
                return "double"
            elif type == "string":
                return "string"
            else:
                logging.error(f"unexpected type: {type}")

        # each field in the schema has name:type:nullable
        schema = list(map(lambda col: f"{col.name}:{convert_col(col.type)}:true", table.sd.cols))
        return ",".join(schema)

    def _create_table_views(self, table):
        fs, path = pyarrow.fs.FileSystem.from_uri(table.sd.location)
        file_info = fs.get_file_info(pyarrow.fs.FileSelector(path))
        files = [f.path for f in file_info if f.is_file and f.size > 0]
        file_path = os.path.split(os.path.split(table.sd.location)[0])[0] + files[0]
        self._table_paths[table.tableName] = file_path
        schema = self._get_schema(table)
        f = fs.open_input_file(file_path)
        reader = pyarrow.parquet.ParquetFile(f)
        # Even when the number of row groups is small, a query can generate multiple
        # queries to the same table.  So we must limit to at least the number of
        # requests that will be arriving to us, which is at least the level of parallelism
        # aka, the number of Spark workers.
        view_count = self._max_views
        logging.info(f"found file: {file_path} row_groups:{reader.num_row_groups} views:{view_count}")
        for request_id in range(0, view_count):
            self._create_view(table, request_id, schema, file_path)

        # Tell the datasource about our table and the number of views it has.
        # This table descriptor will be used later to fetch a request id
        # via fillRequestInfo()
        self._gw.jvm.com.github.qflock.datasource.QflockTableDescriptor.addTable(table.tableName, view_count)
        desc = self._gw.jvm.com.github.qflock.datasource.QflockTableDescriptor.getTableDescriptor(table.tableName)
        self._ds_table_desc[table.tableName] = desc
        f.close()

    def _create_views(self):
        for table in self._tables:
            self._create_table_views(table)
            logging.info(f"View creation Complete. {len(self._cached_views.keys())} total views created.")

    def get_query_id(self):
        query_id = self._query_id
        self._query_id += 1
        return query_id

    def exec_query(self, sql, connection):
        self._lock.acquire()
        query_id = self.get_query_id()
        self._lock.release()
        query = sql.replace('\"', "")
        query_stats = connection['properties']['queryStats']
        rg_offset = connection['properties']['rowGroupOffset']
        rg_count = connection['properties']['rowGroupCount']
        table_name = connection['properties']['tableName']
        
        # get a request_id from the datasource.
        # This will allow us to pass parameters of rg_offset and count
        # down to the datasource, while still querying using an SQL string.
        # Choose the view which represents this request_id
        req_id = self._ds_table_desc[table_name].fillRequestInfo(int(rg_offset), int(rg_count))

        query = query.replace(f" {table_name} ", f" {table_name}_{req_id} ")
        logging.debug(f"query_id: {query_id} req_id: {req_id} table:{table_name} "
                     f"query: {query} ")
        df = self._spark.sql(query)
        df_pandas = df.toPandas()
        num_rows = len(df_pandas.index)
        logging.debug(f"query toPandas() done rows:{num_rows}")
        df_schema = df.schema
        binary_rows = []
        col_type_bytes = []
        col_bytes = []
        comp_rows = []
        col_comp_bytes = []
        if num_rows > 0:
            columns = df.columns
            for col_idx in range(0, len(columns)):
                # data = np_array[:,col_idx]
                data = df_pandas[columns[col_idx]].to_numpy()
                col_name = df_schema.fields[col_idx].name
                data_type = df_schema.fields[col_idx].dataType
                if isinstance(data_type, StringType):
                    new_data1 = data.astype(str)
                    new_data = np.char.encode(new_data1, encoding='utf-8')
                    item_size = new_data.dtype.itemsize
                    num_bytes = len(new_data) * item_size
                    col_bytes.append(num_bytes)
                    if self._compression is True:
                        # logging.debug(f"compressing col:{col_name} type_size:{item_size} rows:{num_rows} bytes: {num_bytes}")
                        new_data = zstd.ZstdCompressor().compress(new_data)
                        col_comp_bytes.append(len(new_data))
                        # logging.debug(f"compressing rows:{num_rows}.  bytes: {num_bytes}:{len(new_data)} Done")
                        raw_bytes = new_data
                        comp_rows.append(raw_bytes)
                    else:
                        raw_bytes = new_data.tobytes()
                        col_comp_bytes.append(len(raw_bytes))
                        binary_rows.append(raw_bytes)
                    col_type_bytes.append(item_size)
                else:
                    new_data = data.byteswap().newbyteorder().tobytes()
                    # new_data = data.tobytes()
                    num_bytes = len(new_data)
                    col_bytes.append(num_bytes)
                    if self._compression is True:
                        # logging.debug(f"compressing col:{col_name} rows:{num_rows} bytes: {num_bytes}")

                        new_data = zstd.ZstdCompressor().compress(new_data)
                        col_comp_bytes.append(len(new_data))
                        comp_rows.append(new_data)
                        # logging.debug(f"compressing rows:{num_rows} bytes: {num_bytes}:{len(new_data)} Done")
                    else:
                        col_comp_bytes.append(len(new_data))
                        binary_rows.append(new_data)
                    col_type_bytes.append(QflockThriftJdbcHandler.data_type_size(data_type))

        stats = query_stats.split(" ")
        if query_stats != "" and len(stats) > 0:
            prevBytes = stats[1].split(":")[1]
            prevRows = stats[2].split(":")[1]
            currentBytes = stats[3].split(":")[1]
            currentRows = stats[4].split(":")[1]
            logging.debug(f"query-done rows:{num_rows} estRows:{currentRows} " +
                         f"estBytes:{currentBytes} " +
                         f"estNoPushBytes:{prevBytes} estNoPushRows:{prevRows} " +
                         f"query: {query}")
        else:
            logging.debug(f"query-done rows:{num_rows} " +
                         f"query: {query}")
        self._ds_table_desc[table_name].freeRequest(req_id)
        # logging.info(f"query-done rows:{num_rows} query: {query}")
        return ttypes.QFResultSet(id=query_id, metadata=self.get_metadata(df_schema),
                                  numRows=num_rows, binaryRows=binary_rows, columnTypeBytes=col_type_bytes,
                                  columnBytes=col_bytes, compressedColumnBytes=col_comp_bytes,
                                  compressedRows=comp_rows)

    def get_connection_id(self):
        current_id = self._connection_id
        self._connection_id += 1
        return current_id

    def createConnection(self, url, properties):
        dbname = url.split(";")[0].lstrip("/")
        self._lock.acquire()
        current_id = self.get_connection_id()
        self._connections[current_id] = {'url': url, 'properties': properties,
                                         'dbname': dbname}
        self._lock.release()
        logging.debug(f"New connection id {current_id} dbname {dbname} url {url} properties {str(properties)}")
        return ttypes.QFConnection(id=current_id)

    def createStatement(self, connection):
        logging.debug(f"createStatement connection id: {connection.id}")
        return ttypes.QFStatement(id=42, sql=None, id_connection=connection.id)

    def get_prepared_statement_id(self):
        current_id = self._pstatement_id
        self._pstatement_id += 1
        return current_id

    def createPreparedStatement(self, connection):
        self._lock.acquire()
        current_id = self.get_prepared_statement_id()
        self._pstatements[current_id] = {'connection': connection}
        self._lock.release()
        logging.debug(f"createPreparedStatement id {current_id} connection id: {connection.id}")
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
            logging.debug(f"successfully closed connection {connection.id}")
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
            logging.debug(f"successfully closed preparedStatement {statement.id}")
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
        logging.debug(f"preparedStatement_executeQuery:: statement id: {statement.id} conn id: {connection_id} " +\
                     f" offset:{connection['properties']['rowGroupOffset']} " +\
                     f" count:{connection['properties']['rowGroupCount']} " +\
                     f" tableName:{connection['properties']['tableName']} sql: {sql}")
        # row = ttypes.QFRow([ttypes.QFValue(isnull=False, val=ttypes.RawVal(integer_val=42))])
        # rows = [row]
        # # 4 = java.sql.Types.INTEGER (JdbcUtil.getSchema
        # columnName = 'fakecol1'
        # parts = [ttypes.QFResultSetMetaDataPart(columnName=columnName, columnType=4,
        #                                         columnLabel=columnName, signed=False)]
        # metadata = ttypes.QFResultSetMetaData(parts)
        # return ttypes.QFResultSet(42, rows, metadata)
        self._spark.sql(f"USE {connection['dbname']}")
        return self.exec_query(sql, connection)

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







import sys
import json
import subprocess
import os
import logging
import inspect
from enum import Enum
import pyspark
from pyspark.sql.types import StringType, DoubleType, IntegerType, LongType

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

my_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(my_path + '/jdbc-thrift')

from com.github.qflock.jdbc.api import QflockJdbcService
from com.github.qflock.jdbc.api import ttypes

global logger


class DataType(Enum):
    STRING = 1
    DOUBLE = 2
    LONG = 3
    INTEGER = 4


def schema_to_types(schema):
    type_array = []
    for field in schema.fields:
        if isinstance(field.dataType, StringType):
            type_array.append(DataType.STRING)
        elif isinstance(field.dataType, DoubleType):
            type_array.append(DataType.DOUBLE)
        elif isinstance(field.dataType, LongType):
            type_array.append(DataType.LONG)
        elif isinstance(field.dataType, IntegerType):
            type_array.append(DataType.INTEGER)
        else:
            print("unknown type")
            raise Exception(f"unknown type {field.dataType}")
    return type_array


def map_value1(data_type, value):
    if data_type == DataType.STRING:
        return ttypes.RawVal(string_val=value)
    elif data_type == DataType.DOUBLE:
        return ttypes.RawVal(double_val=value)
    elif data_type == DataType.INTEGER:
        return ttypes.RawVal(integer_val=value)
    elif data_type == DataType.LONG:
        return ttypes.RawVal(integer_val=value)
    else:
        print("unknown type")
        raise Exception(f"unknown type {data_type}")


def map_row1(row_data, types):
    new_row = []
    for col_idx in range(0, len(types)):
        value = map_value1(types[col_idx], row_data[col_idx])
        if row_data[col_idx]:
            new_row.append(ttypes.QFValue(isnull=False, val=value))
        else:
            new_row.append(ttypes.QFValue(isnull=True, val=None))
    return new_row


class ThriftJdbcHandler:
    def __init__(self):
        self._connections = {}
        self._pstatements = {}
        self._connection_id = 0
        self._pstatement_id = 0
        # Inspired by https://thrift.apache.org/tutorial/py.html
        self._spark = pyspark.sql.SparkSession \
            .builder \
            .appName("qflock-jdbc") \
            .config("spark.driver.maxResultSize", "2g")\
            .config("spark.driver.maxResultSize", "2g")\
            .config("spark.driver.memory", "2g")\
            .config("spark.executor.memory", "2g")\
            .config("spark.sql.catalogImplementation", "hive")\
            .config("spark.sql.warehouse.dir", "hdfs://qflock-storage:9000/user/hive/warehouse3")\
            .config("spark.hadoop.hive.metastore.uris", "thrift://qflock-storage:9084")\
            .enableHiveSupport() \
            .getOrCreate()

    def createConnection(self, url, properties):
        current_id = self._connection_id
        dbname = url.split(";")[0].lstrip("/")
        logger.info(f"New connection id {current_id} dbname {dbname} url {url} properties {str(properties)}")
        self._connection_id += 1
        self._connections[current_id] = {'url': url, 'properties': properties,
                                         'dbname': dbname }
        return ttypes.QFConnection(id=current_id)

    def createStatement(self, connection):
        logger.info(f"createStatement connection id: {connection.id}")
        return ttypes.QFStatement(id=42, sql=None, id_connection=connection.id)

    def createPreparedStatement(self, connection):
        current_id = self._pstatement_id
        logger.info(f"createStatement id {current_id} connection id: {connection.id}")
        self._pstatement_id += 1
        self._pstatements[current_id] = {'connection': connection}
        return ttypes.QFStatement(id=current_id, sql=None, id_connection=connection.id)

    def connection_getstaticmetadata(self, connection):
        """
        Parameters:
         - connection

        """
        logger.debug("connection_getstaticmetadata")

    def connection_isvalid(self, connection, timeout):
        """
        Parameters:
         - connection
         - timeout

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def connection_setAutoCommit(self, connection, autoCommit):
        """
        Parameters:
         - connection
         - autoCommit

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def connection_getAutoCommit(self, connection):
        """
        Parameters:
         - connection

        """
        logger.debug("connection_getAutoCommit::")
        return QflockJdbcService.connection_getAutoCommit_result(success=True)

    def connection_setTransactionIsolation(self, connection, level):
        """
        Parameters:
         - connection
         - level

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def connection_getTransactionIsolation(self, connection):
        """
        Parameters:
         - connection

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def connection_setReadOnly(self, connection, readOnly):
        """
        Parameters:
         - connection
         - readOnly

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def connection_getReadOnly(self, connection):
        """
        Parameters:
         - connection

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def connection_setCatalog(self, connection, catalog):
        """
        Parameters:
         - connection
         - catalog

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def connection_getCatalog(self, connection):
        """
        Parameters:
         - connection

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def connection_setSchema(self, connection, schema):
        """
        Parameters:
         - connection
         - schema

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def connection_getSchema(self, connection):
        """
        Parameters:
         - connection

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def connection_getCatalogSeparator(self, connection):
        """
        Parameters:
         - connection

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def connection_getCatalogTerm(self, connection):
        """
        Parameters:
         - connection

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def connection_getSchemaTerm(self, connection):
        """
        Parameters:
         - connection

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def connection_getCatalogs(self, connection):
        """
        Parameters:
         - connection

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def connection_getSchemas(self, connection, catalog, schemaPattern):
        """
        Parameters:
         - connection
         - catalog
         - schemaPattern

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def connection_getTables(self, connection, catalog, schemaPattern, tableNamePattern, types):
        """
        Parameters:
         - connection
         - catalog
         - schemaPattern
         - tableNamePattern
         - types

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def connection_getColumns(self, connection, catalog, schemaPattern, tableNamePattern, columnNamePattern):
        """
        Parameters:
         - connection
         - catalog
         - schemaPattern
         - tableNamePattern
         - columnNamePattern

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def connection_getSQLKeywords(self, connection):
        """
        Parameters:
         - connection

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def connection_getTableTypes(self, connection):
        """
        Parameters:
         - connection

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def connection_getTypeInfo(self, connection):
        """
        Parameters:
         - connection

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def closeConnection(self, connection):
        """
        Parameters:
         - connection

        """
        if connection.id in self._connections:
            del self._connections[connection.id]
            logger.info(f"successfully closed connection {connection.id}")
        else:
            logger.warning(f"connection id {connection.id} not found")

    def statement_close(self, statement):
        """
        Parameters:
         - statement

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def statement_execute(self, statement, sql):
        """
        Parameters:
         - statement
         - sql

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def statement_executeQuery(self, statement, sql):
        """
        Parameters:
         - statement
         - sql

        """
        logger.debug(f"statement_executeQuery:: statement id: {statement.id} sql: {sql}")
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

    def map_row(self, row_data, df_schema):
        new_row = []
        col_idx = 0
        for field in df_schema.fields:
            value = self.map_value(field.dataType, row_data[col_idx])
            if row_data[col_idx]:
                new_row.append(ttypes.QFValue(isnull=False, val=value))
            else:
                new_row.append(ttypes.QFValue(isnull=True, val=None))
            col_idx += 1
        return new_row

    def exec_query1(self, sql):
        df = self._spark.sql(sql.replace('\"', ""))
        logger.info(f"query starting")
        df_schema = df.schema
        data_rows = df.collect()
        # logger.info(f"query complete {len(data_rows)} rows")
        # logger.info(f"prepare result starting")
        data_types = schema_to_types(df_schema)
        rows = []
        for row in data_rows[0:1000]:
            rows.append(ttypes.QFRow(map_row1(row, data_types)))
        # rows = []
        # for row in data_rows[0:1000]:
        #     row_value = self.map_row(row, df_schema)
        #     rows.append(ttypes.QFRow(row_value))
        logger.info(f"prepare result finished for {len(rows)} rows")
        return ttypes.QFResultSet(42, rows, self.get_metadata(df_schema))

    def exec_query(self, sql):
        query = sql.replace('\"', "") + " LIMIT 1000"
        logger.info(f"query starting {query}")
        df = self._spark.sql(query)
        def custom_function(row, schema):
            row_value = map_row1(row, schema)
            return (ttypes.QFRow(row_value))
        df_schema = df.schema
        data_types = schema_to_types(df_schema)
        # new_rows = df.rdd.map(lambda x: (x[0], ))
        #new_rows = df.rdd.map(lambda x: (ttypes.QFRow(ttypes.RawVal(double_val=x[0])),))
        #new_rows = df.rdd.map(lambda x: custom_function(x, data_types))
        new_rows = df.rdd.map(lambda x: ttypes.QFRow(map_row1(x, data_types)))
        if new_rows.isEmpty():
            rows = []
        else:
            #rows = new_rows.toDF(['ss_sold_date_sk']).collect()
            rows = new_rows.collect()
        logger.info(f"query finished for {len(rows)} rows")
        return ttypes.QFResultSet(42, rows, self.get_metadata(df_schema))

    def statement_getResultSet(self, statement):
        """
        Parameters:
         - statement

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def statement_getUpdateCount(self, statement):
        """
        Parameters:
         - statement

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def statement_getResultSetType(self, statement):
        """
        Parameters:
         - statement

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def statement_cancel(self, statement):
        """
        Parameters:
         - statement

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def statement_getWarnings(self, statement):
        """
        Parameters:
         - statement

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def statement_clearWarnings(self, statement):
        """
        Parameters:
         - statement

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def statement_getMaxRows(self, statement):
        """
        Parameters:
         - statement

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def statement_setMaxRows(self, statement, max):
        """
        Parameters:
         - statement
         - max

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def statement_getQueryTimeout(self, statement):
        """
        Parameters:
         - statement

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def statement_setQueryTimeout(self, statement, seconds):
        """
        Parameters:
         - statement
         - seconds

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def preparedStatement_close(self, statement):
        """
        Parameters:
         - statement

        """
        if statement.id in self._pstatements:
            del self._pstatements[statement.id]
            logger.info(f"successfully closed preparedStatement {statement.id}")
        else:
            logger.warning(f"preparedStatement id {statement.id} not found")

    def preparedStatement_execute(self, statement, sql):
        """
        Parameters:
         - statement
         - sql

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def preparedStatement_executeQuery(self, statement, sql):
        """
        Parameters:
         - statement
         - sql

        """
        connection_id = self._pstatements[statement.id]['connection'].id
        connection = self._connections[connection_id]
        logger.debug(f"preparedStatement_executeQuery:: statement id: {statement.id} sql: {sql}" +\
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
        logger.debug(inspect.currentframe().f_code.co_name)

    def preparedStatement_getUpdateCount(self, statement):
        """
        Parameters:
         - statement

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def preparedStatement_getResultSetType(self, statement):
        """
        Parameters:
         - statement

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def preparedStatement_cancel(self, statement):
        """
        Parameters:
         - statement

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def preparedStatement_getWarnings(self, statement):
        """
        Parameters:
         - statement

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def preparedStatement_clearWarnings(self, statement):
        """
        Parameters:
         - statement

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def preparedStatement_getMaxRows(self, statement):
        """
        Parameters:
         - statement

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def preparedStatement_setMaxRows(self, statement, max):
        """
        Parameters:
         - statement
         - max

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def preparedStatement_getQueryTimeout(self, statement):
        """
        Parameters:
         - statement

        """
        logger.debug(inspect.currentframe().f_code.co_name)

    def preparedStatement_setQueryTimeout(self, statement, seconds):
        """
        Parameters:
         - statement
         - seconds

        """
        logger.debug(inspect.currentframe().f_code.co_name)



def get_jdbc_ip():
    if os.getenv('RUNNING_MODE') is not None:
        return 'localhost'

    result = subprocess.run('docker network inspect qflock-net'.split(' '), stdout=subprocess.PIPE)
    d = json.loads(result.stdout)

    return d[0]['IPAM']['Config'][0]['Gateway']

def setup_logger():
    # create logger
    logger = logging.getLogger("qflock")
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # add ch to logger
    logger.addHandler(ch)

    # create formatter
    formatter = logging.Formatter("%(asctime)s.%(msecs)03d %(levelname)s %(message)s",
                                  "%Y-%m-%d %H:%M:%S")

    # add formatter to ch
    ch.setFormatter(formatter)



if __name__ == '__main__':

    setup_logger()
    jdbc_port = 1433
    jdbc_ip = get_jdbc_ip()
    handler = ThriftJdbcHandler()
    processor = QflockJdbcService.Processor(handler)
    transport = TSocket.TServerSocket(host=jdbc_ip, port=jdbc_port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    logger = logging.getLogger("qflock")
    logger.info(f'Starting the JDBC server...{jdbc_ip}:{jdbc_port}')
    try:
        server.serve()
    except BaseException as ex:
        pass
import sys
import glob
import json
import subprocess
import functools
import os
import time
import logging

from thrift import Thrift
from thrift.TSerialization import serialize
from thrift.TSerialization import deserialize
from thrift.protocol.TJSONProtocol import TSimpleJSONProtocolFactory
from thrift.protocol.TJSONProtocol import TJSONProtocolFactory

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.server import TNonblockingServer
from thrift.protocol.THeaderProtocol import THeaderProtocolFactory

import pyarrow.parquet
import pyarrow.fs

my_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(my_path + '/pymetastore')

from hive_metastore import ThriftHiveMetastore
from hive_metastore import ttypes


def log(*args):
    if True:
        print(*args, flush=True)

catalog = "default"
    
class ThriftHiveMetastoreHandler:
    def __init__(self):
        # Inspired by https://thrift.apache.org/tutorial/py.html
        self.storage_ip = get_storage_ip()
        client_transport = TSocket.TSocket(self.storage_ip, 9083)
        client_transport = TTransport.TBufferedTransport(client_transport)
        client_protocol = TBinaryProtocol.TBinaryProtocol(client_transport)
        client = ThriftHiveMetastore.Client(client_protocol)
        # client_transport.open()
        self.client = client
        # self.client_transport = client_transport

        self.logfile = open('/opt/volume/metastore/qflock/logfile.log', 'w')

    def logv(self, attr, args, res):
        self.logfile.write(attr)
        for arg in args:
            self.logfile.write(str(type(arg)) + " " + str(arg)[:20])

        self.logfile.write(str(type(res)) + '\n')
        self.logfile.flush()

    def close(self):
        # log('Closing client transport ...')
        # self.client_transport.close()
        pass

    def add_qflock_statistics(self, table: ttypes.Table):
        log(f'Adding statistics for {table.sd.location}')
        storage_size = 0
        fs, path = pyarrow.fs.FileSystem.from_uri(table.sd.location)
        file_info = fs.get_file_info(pyarrow.fs.FileSelector(path))
        # PEP 572 – Assignment Expressions
        tmp = [storage_size := storage_size + f.size for f in file_info if f.is_file]
        table.sd.parameters['qflock.storage_size'] = str(storage_size)

        files = [finfo.path for finfo in file_info if finfo.is_file and finfo.size > 0]

        f = fs.open_input_file(files[0])
        reader = pyarrow.parquet.ParquetFile(f)

        rg = reader.metadata.row_group(0)
        for col in range(0, rg.num_columns):
            col_info = rg.column(col)
            bytes_per_row = round(col_info.total_compressed_size / rg.num_rows, 2)
            param_key = f'spark.qflock.statistics.colStats.{col_info.path_in_schema}.bytes_per_row'
            table.parameters[param_key] = str(bytes_per_row)
            log(col, param_key, bytes_per_row)

        param_key = f'spark.qflock.statistics.tableStats.{table.tableName}.row_groups'
        table.parameters[param_key] = str(reader.metadata.num_row_groups)
        f.close()

    def write_database(self, db_name: str, database: ttypes.Database):
        db_path = f'/opt/volume/metastore/qflock/catalog/{catalog}/{db_name}'
        if not os.path.exists(db_path):
            os.mkdir(db_path)

        f = open(f'{db_path}/{db_name}.database.json', 'wb')
        data = serialize(database, protocol_factory=TJSONProtocolFactory())
        f.write(data)
        f.close()

    def read_database(self, db_name: str):
        path = f'/opt/volume/metastore/qflock/catalog/{catalog}/{db_name}/{db_name}.database.json'

        if not os.path.exists(path):
            raise ThriftHiveMetastore.NoSuchObjectException(db_name)

        f = open(path, 'rb')
        data = f.read()
        f.close()

        database = ttypes.Database()
        deserialize(database, data,  protocol_factory=TJSONProtocolFactory())
        return database

    def write_table(self, db_name: str, table_name: str, table: ttypes.Table):
        db_path = f'/opt/volume/metastore/qflock/catalog/{catalog}/{db_name}'
        if not os.path.exists(db_path):
            os.mkdir(db_path)

        f = open(f'/opt/volume/metastore/qflock/catalog/{catalog}/{db_name}/{table_name}.table.json', 'wb')
        data = serialize(table, protocol_factory=TJSONProtocolFactory())
        f.write(data)
        f.close()

    def read_table(self, db_name: str, table_name: str):
        path = f'/opt/volume/metastore/qflock/catalog/{catalog}/{db_name}/{table_name}.table.json'

        if not os.path.exists(path):
            raise ThriftHiveMetastore.NoSuchObjectException(f'{db_name}.{table_name}')

        f = open(path, 'rb')
        data = f.read()
        f.close()

        table = ttypes.Table()
        deserialize(table, data, protocol_factory=TJSONProtocolFactory())
        return table

    def _decorator(self, f, attr):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            try:
                if attr == 'set_ugi':
                    return list()

                if attr == 'create_database':
                    self.write_database(args[0].name, args[0])
                    return None

                if attr == 'create_table_with_environment_context':
                    self.write_table(args[0].dbName, args[0].tableName, args[0])
                    return None
                    
                if attr == 'alter_table_with_environment_context':
                    self.add_qflock_statistics(args[2])
                    self.write_table(args[0], args[1], args[2])
                    return None

                if attr == 'alter_table':
                    self.add_qflock_statistics(args[2])
                    self.write_table(args[0], args[1], args[2])
                    return None

                if attr == 'get_databases':
                    databases = list()
                    # args[0] is a string to filter databases '*' is used by Spark
                    path = f'/opt/volume/metastore/qflock/catalog/{catalog}'
                    for (dirpath, dirnames, filenames) in os.walk(path):
                        for dir in dirnames:
                            databases.append(dir)

                    return databases

                if attr == 'get_all_databases':
                    databases = list()
                    path = f'/opt/volume/metastore/qflock/catalog/{catalog}'
                    for (dirpath, dirnames, filenames) in os.walk(path):
                        for dir in dirnames:                            
                                databases.append(dir)

                    return databases

                if attr == 'get_tables':
                    tables = list()
                    # args[0] is a string to filter tables '*' is used by Spark
                    path = f'/opt/volume/metastore/qflock/catalog/{catalog}/{args[0]}'
                    for (dirpath, dirnames, filenames) in os.walk(path):
                        for fname in filenames:
                            if '.table.json' in fname:
                                tables.append(fname.split('.')[0])

                    return tables

                if attr == 'get_all_tables':
                    tables = list()
                    path = f'/opt/volume/metastore/qflock/catalog/{catalog}/{args[0]}'
                    for (dirpath, dirnames, filenames) in os.walk(path):
                        for fname in filenames:
                            if '.table.json' in fname:
                                tables.append(fname.split('.')[0])

                    return tables

                if attr == 'get_database':
                    return self.read_database(args[0])

                if attr == 'get_table':
                    return self.read_table(args[0], args[1])

                # res = f(*args, **kwargs)
            except BaseException as error:
                # log('An exception occurred: {}'.format(error))
                raise error

            self.logv(attr, args, 'Unsupported!!!')
            return None

        return wrapper

    def __getattr__(self, attr):
        f = getattr(self.client, attr)
        decorator = object.__getattribute__(self, '_decorator')
        return decorator(f, attr)


class ThriftHiveMetastoreServer(TServer.TThreadedServer):
    def handle(self, client):
        itrans = self.inputTransportFactory.getTransport(client)
        iprot = self.inputProtocolFactory.getProtocol(itrans)

        handler = ThriftHiveMetastoreHandler()
        processor = ThriftHiveMetastore.Processor(handler)

        # for THeaderProtocol, we must use the same protocol instance for input
        # and output so that the response is in the same dialect that the
        # server detected the request was in.
        if isinstance(self.inputProtocolFactory, THeaderProtocolFactory):
            otrans = None
            oprot = iprot
        else:
            otrans = self.outputTransportFactory.getTransport(client)
            oprot = self.outputProtocolFactory.getProtocol(otrans)

        try:
            while True:
                processor.process(iprot, oprot)
        except TTransport.TTransportException:
            pass
        except Exception as x:
            logger.exception(x)

        itrans.close()
        if otrans:
            otrans.close()

        handler.close()


def get_storage_ip():
    if os.getenv('RUNNING_MODE') is not None:
        return 'localhost'

    result = subprocess.run('docker network inspect qflock-net'.split(' '), stdout=subprocess.PIPE)
    d = json.loads(result.stdout)

    for c in d[0]['Containers'].values():
        log(c['Name'], c['IPv4Address'].split('/')[0])
        if c['Name'] == 'qflock-storage':
            addr = c['IPv4Address'].split('/')[0]
            with open('host_aliases', 'w') as f:
                f.write(f'qflock-storage {addr}')

            os.environ.putenv('HOSTALIASES', 'host_aliases')
            os.environ['HOSTALIASES'] = 'host_aliases'
            return addr

    return None


if __name__ == '__main__':
    storage_ip = get_storage_ip()

    server_transport = TSocket.TServerSocket(host=None, port=9084)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = ThriftHiveMetastoreServer(None, server_transport, tfactory, pfactory)

    log('Starting the server...')
    try:
        server.serve()
    except BaseException as ex:
        pass






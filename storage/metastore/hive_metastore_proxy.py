import sys
import glob
import json
import subprocess
import functools
import os
import time
import logging

from thrift import Thrift
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
    if False:
        print(*args, flush=True)
    
class ThriftHiveMetastoreHandler:
    def __init__(self):
        # Inspired by https://thrift.apache.org/tutorial/py.html
        self.storage_ip = get_storage_ip()
        client_transport = TSocket.TSocket(self.storage_ip, 9083)
        client_transport = TTransport.TBufferedTransport(client_transport)
        client_protocol = TBinaryProtocol.TBinaryProtocol(client_transport)
        client = ThriftHiveMetastore.Client(client_protocol)
        client_transport.open()
        self.client = client
        self.client_transport = client_transport

    def close(self):
        # log('Closing client transport ...')
        self.client_transport.close()

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

    def _decorator(self, f, attr):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            if attr == 'alter_table_with_environment_context':
                log(attr, args[:2])
                self.add_qflock_statistics(args[2])
            else:
                log(attr, args, kwargs)

            try:
                res = f(*args, **kwargs)
            except BaseException as error:
                log('An exception occurred: {}'.format(error))
                raise error

            return res

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

def parquet_test():
    fname = f'hdfs://qflock-storage:9000/tpcds-parquet/call_center.parquet'

    fs, path = pyarrow.fs.FileSystem.from_uri(fname)
    file_info = fs.get_file_info(pyarrow.fs.FileSelector(path))
    files = [finfo.path for finfo in file_info if finfo.is_file and finfo.size > 0]

    f = fs.open_input_file(files[0])
    reader = pyarrow.parquet.ParquetFile(f)

    rg = reader.metadata.row_group(0)
    for col in range(0, rg.num_columns):
        col_info = rg.column(col)
        bytes_per_row = round(col_info.total_compressed_size / rg.num_rows, 2)
        param_key = f'spark.qflock.statistics.colStats.{col_info.path_in_schema}.bytes_per_row'
        log(col, param_key, bytes_per_row)

    f.close()

if __name__ == '__main__':
    storage_ip = get_storage_ip()

    # Inspired by https://thrift.apache.org/tutorial/py.html
    client_transport = TSocket.TSocket(storage_ip, 9083)
    client_transport = TTransport.TBufferedTransport(client_transport)
    client_protocol = TBinaryProtocol.TBinaryProtocol(client_transport)
    client = ThriftHiveMetastore.Client(client_protocol)

    logging.disable(logging.CRITICAL)
    while not client_transport.isOpen():
        try:
            client_transport.open()
        except BaseException as ex:
            log('Metastore is not ready. Retry in 1 sec.')
            time.sleep(1)

    logging.disable(logging.NOTSET)

    catalogs = client.get_catalogs()
    log(catalogs)
    client_transport.close()

    server_transport = TSocket.TServerSocket(host=None, port=9084)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = ThriftHiveMetastoreServer(None, server_transport, tfactory, pfactory)

    # server = TServer.TSimpleServer(processor, server_transport, tfactory, pfactory)
    '''
    TThreadedServer spawns a new thread for each client connection
    '''
    # server = TServer.TThreadedServer(processor, server_transport, tfactory, pfactory)

    '''
    TNonblockingServer has one thread dedicated for network I/O. 
    The same thread can also process requests, or you can create a separate pool of worker threads for request processing. 
    The server can handle many concurrent connections with a small number of threads since it doesn’t need to spawn a new thread for each connection.
    '''
    # server = TNonblockingServer.TNonblockingServer(processor, server_transport, tfactory, pfactory)

    '''
    TThreadPoolServer is similar to TThreadedServer; each client connection gets its own dedicated server thread. 
    It’s different from TThreadedServer in 2 ways:
    Server thread goes back to the thread pool after client closes the connection for reuse. 
    There is a limit on the number of threads. 
    The thread pool won’t grow beyond the limit. Client hangs if there is no more thread available in the thread pool. 
    It’s much more difficult to use compared to the other 2 servers.'''
    # server = TServer.TThreadPoolServer(processor, server_transport, tfactory, pfactory)

    log('Starting the server...')
    try:
        server.serve()
    except BaseException as ex:
        pass






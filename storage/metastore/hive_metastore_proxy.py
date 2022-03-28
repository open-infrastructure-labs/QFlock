import sys
import glob
import json
import subprocess
import functools
import os

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.server import TNonblockingServer
from thrift.protocol.THeaderProtocol import THeaderProtocolFactory

my_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(my_path + '/pymetastore')

from hive_metastore import ThriftHiveMetastore
from hive_metastore import ttypes

class ThriftHiveMetastoreHandler:
    def __init__(self):
        # Inspired by https://thrift.apache.org/tutorial/py.html
        storage_ip = get_storage_ip()
        client_transport = TSocket.TSocket(storage_ip, 9083)
        client_transport = TTransport.TBufferedTransport(client_transport)
        client_protocol = TBinaryProtocol.TBinaryProtocol(client_transport)
        client = ThriftHiveMetastore.Client(client_protocol)
        client_transport.open()
        self.client = client
        self.client_transport = client_transport

    def close(self):
        # print('Closing client transport ...')
        self.client_transport.close()

    def _decorator(self, f, attr):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            print(attr, args, kwargs)
            try:
                res = f(*args, **kwargs)
            except BaseException as error:
                print('An exception occurred: {}'.format(error))
                raise error

            if isinstance(res, ttypes.GetTableResult):
                for c in res.table.sd.cols:
                    res.table.parameters[f'spark.sql.statistics.colStats.{c.name}.bytes_per_row'] = '0.5'
                # print(res.table.parameters)
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
    return 'localhost'
    result = subprocess.run('docker network inspect qflock-net'.split(' '), stdout=subprocess.PIPE)
    d = json.loads(result.stdout)

    for c in d[0]['Containers'].values():
        print(c['Name'], c['IPv4Address'].split('/')[0])
        if c['Name'] == 'qflock-storage':
            return c['IPv4Address'].split('/')[0]

    return None


if __name__ == '__main__':
    # Inspired by https://thrift.apache.org/tutorial/py.html
    storage_ip = get_storage_ip()
    # storage_ip = 'localhost'
    client_transport = TSocket.TSocket(storage_ip, 9083)
    client_transport = TTransport.TBufferedTransport(client_transport)
    client_protocol = TBinaryProtocol.TBinaryProtocol(client_transport)
    client = ThriftHiveMetastore.Client(client_protocol)
    client_transport.open()

    catalogs = client.get_catalogs()
    print(catalogs)
    client_transport.close()

    # handler = ThriftHiveMetastoreHandler(client)
    # processor = ThriftHiveMetastore.Processor(handler)
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

    print('Starting the server...')
    try:
        server.serve()
    except BaseException as ex:
        pass






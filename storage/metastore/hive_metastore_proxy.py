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

my_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(my_path + '/pymetastore')

from hive_metastore import ThriftHiveMetastore
from hive_metastore import ttypes

class ThriftHiveMetastoreHandler:
    def __init__(self, client):
        self.client = client

    def _decorator(self, f, attr):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            res = f(*args, **kwargs)
            # print(attr, args, kwargs)
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


def get_storage_ip():
    result = subprocess.run('docker network inspect qflock-net'.split(' '), stdout=subprocess.PIPE)
    d = json.loads(result.stdout)

    for c in d[0]['Containers'].values():
        print(c['Name'], c['IPv4Address'].split('/')[0])
        if c['Name'] == 'qflock-storage':
            return c['IPv4Address'].split('/')[0]

    return None


if __name__ == '__main__':
    # Inspired by https://thrift.apache.org/tutorial/py.html
    # storage_ip = get_storage_ip()
    storage_ip = 'localhost'
    client_transport = TSocket.TSocket(storage_ip, 9083)
    client_transport = TTransport.TBufferedTransport(client_transport)
    client_protocol = TBinaryProtocol.TBinaryProtocol(client_transport)
    client = ThriftHiveMetastore.Client(client_protocol)
    client_transport.open()

    catalogs = client.get_catalogs()
    print(catalogs)

    handler = ThriftHiveMetastoreHandler(client)
    processor = ThriftHiveMetastore.Processor(handler)
    server_transport = TSocket.TServerSocket(host=None, port=9084)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TSimpleServer(processor, server_transport, tfactory, pfactory)

    print('Starting the server...')
    server.serve()

    # Close!
    client_transport.close()




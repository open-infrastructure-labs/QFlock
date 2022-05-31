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
import sys
import os
import logging
import time
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.server import TNonblockingServer
from thrift.protocol.THeaderProtocol import THeaderProtocolFactory

my_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.realpath(my_path + "/../../../storage/metastore/pymetastore/"))
from hive_metastore import ThriftHiveMetastore
from hive_metastore import ttypes

class HiveMetastoreClient:
    def __init__(self, metastore_ip, metastore_port):
        logging.info(f"Connecting to metastore: {metastore_ip}:{metastore_port}")
        self._metastore_ip = metastore_ip
        self._metastore_port = metastore_port
        # Inspired by https://thrift.apache.org/tutorial/py.html
        client_transport = TSocket.TSocket(self._metastore_ip, self._metastore_port)
        client_transport = TTransport.TBufferedTransport(client_transport)
        client_protocol = TBinaryProtocol.TBinaryProtocol(client_transport)
        self.client = ThriftHiveMetastore.Client(client_protocol)

        logging.disable(logging.CRITICAL)
        while not client_transport.isOpen():
            try:
                client_transport.open()
            except BaseException as ex:
                logging.warn('Metastore is not ready. Retry in 1 sec.')
                time.sleep(1)
        logging.disable(logging.NOTSET)
        logging.info(f"Successfully connected to metastore: {metastore_ip}:{metastore_port}")

if __name__ == "__main__":
    client = HiveMetastoreClient("qflock-storage-dc1", "9084")

    tables = client.client.get_all_tables("tpcds")

    print(tables)
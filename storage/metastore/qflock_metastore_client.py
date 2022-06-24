import sys
import glob
import subprocess
import functools
import os
import time
import urllib.parse
import http.client
import json

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


def get_docker_ip(docker_name: str):
    result = subprocess.run('docker network inspect qflock-net'.split(' '), stdout=subprocess.PIPE)
    d = json.loads(result.stdout)

    f = open('host_aliases', 'w')
    for c in d[0]['Containers'].values():
        name = c['Name']
        addr = c['IPv4Address'].split('/')[0]
        f.write(f'{name} {addr}\n')

    f.close()
    os.environ.putenv('HOSTALIASES', 'host_aliases')
    os.environ['HOSTALIASES'] = 'host_aliases'

    for c in d[0]['Containers'].values():
        if c['Name'] == docker_name:
            addr = c['IPv4Address'].split('/')[0]
            return addr

    return None

def get_bytes_read(datanode_name: str):
    conn = http.client.HTTPConnection(f'{datanode_name}:9864')
    req = f'/jmx?qry=Hadoop:service=DataNode,name=DataNodeActivity-{datanode_name}-9866'
    conn.request("GET", req)
    resp = conn.getresponse()
    resp_data = resp.read()
    d = json.loads(resp_data)
    conn.close()
    return d['beans'][0]['BytesRead']


def get_column_sizes(location: str):
    print(location)
    # Open parquet file
    fs, path = pyarrow.fs.FileSystem.from_uri(location)
    file_info = fs.get_file_info(pyarrow.fs.FileSelector(path))
    files = [f.path for f in file_info if f.is_file and f.size > 0]

    f = fs.open_input_file(files[0])
    reader = pyarrow.parquet.ParquetFile(f)
    print(f'num_rows: {reader.metadata.num_rows}')
    rg = reader.metadata.row_group(0)
    col_sizes = [0] * rg.num_columns
    for rgi in range(0, reader.num_row_groups):
        rg = reader.metadata.row_group(rgi)
        for col_idx in range(0, rg.num_columns):
            col_info = rg.column(col_idx)
            col_sizes[col_idx] += col_info.total_compressed_size

    for col_idx in range(0, rg.num_columns):
        col_info = rg.column(col_idx)
        print(col_info.path_in_schema, col_sizes[col_idx])

    f.close()

def get_file_stats(location: str):
    print(location)
    # Open parquet file
    fs, path = pyarrow.fs.FileSystem.from_uri(location)
    file_info = fs.get_file_info(pyarrow.fs.FileSelector(path))
    files = [f.path for f in file_info if f.is_file and f.size > 0]

    f = fs.open_input_file(files[0])
    reader = pyarrow.parquet.ParquetFile(f)
    # print(f'num_rows: {reader.metadata.num_rows} num_row_groups: {reader.num_row_groups}')
    num_rows = reader.metadata.num_rows
    num_row_groups = reader.num_row_groups
    f.close()
    return num_rows, num_row_groups



if __name__ == '__main__':
    datanode_name = 'qflock-storage-dc1'
    storage_ip = get_docker_ip(datanode_name)
    client_transport = TSocket.TSocket(storage_ip, 9084)
    client_transport = TTransport.TBufferedTransport(client_transport)
    client_protocol = TBinaryProtocol.TBinaryProtocol(client_transport)
    client = ThriftHiveMetastore.Client(client_protocol)

    while not client_transport.isOpen():
        try:
            client_transport.open()
        except BaseException as ex:
            print('Metastore is not ready. Retry in 1 sec.')
            time.sleep(1)

    databases = client.get_all_databases()
    print(databases)

    db_name = 'tpcds'
    tpcds = client.get_database(db_name)
    print(tpcds)

    table_names = client.get_all_tables(db_name)
    print(table_names)

    tables = [client.get_table(db_name, table_name) for table_name in table_names]
    tables.sort(key=lambda tbl: int(tbl.sd.parameters['qflock.storage_size']), reverse=True)

    print("path,data center,bytes,rows")
    for tbl in tables:
        dc = "dc1" if "dc1" in tbl.sd.location else "dc2"
        rows, row_groups = get_file_stats(tbl.sd.location)
        stat_name = f"spark.qflock.statistics.tableStats.{tbl.tableName}.row_groups"
        print(f"{tbl.sd.location},{dc},{tbl.sd.parameters['qflock.storage_size']},"
              f"{tbl.parameters['spark.sql.statistics.numRows']},"
              f"{tbl.parameters[stat_name]},{rows},{row_groups}")


    get_column_sizes(tables[0].sd.location)
    client_transport.close()

'''
benchmark/src/docker-bench.py --query_text "select cs_sold_date_sk from catalog_sales" --verbose
benchmark/src/docker-bench.py --query_text "select cs_sold_time_sk from catalog_sales" --verbose
cs_sold_date_sk 43828
cs_sold_time_sk 1337719
cs_ship_date_sk 1885012
cs_bill_customer_sk 1593327
cs_bill_cdemo_sk 2036827

benchmark/src/docker-bench.py --query_text "select * from store_sales" 
'''

import time
import threading
import urllib.parse
import subprocess
import json
import re

import fastparquet
from fastparquet import core

from webhdfs import WebHdfsFile

import pyarrow.parquet
import numpy


reader_cache = dict()
reader_cache_lock = threading.Lock()


def get_reader(path: str, storage_ip=None):
    reader_cache_lock.acquire()
    if path not in reader_cache.keys():
        reader_cache[path] = ParquetReader(path, storage_ip)
        # reader_cache[path] = ArrowParquetReader(path, storage_ip)
    reader_cache_lock.release()

    return reader_cache[path]


class ParquetReader(fastparquet.ParquetFile):
    def __init__(self, path: str, storage_ip=None):
        self.infile = WebHdfsFile(path, storage_ip=storage_ip)
        super(ParquetReader, self).__init__(self.infile)
        self.dtypes = self.dtypes.values()
        self.num_row_groups = len(self.row_groups)
        self.infile.close()

    def read_rg(self, index: int, columns: list):
        rg = self.row_groups[index]
        #  Allocate memory
        df, assign = self.pre_allocate(rg.num_rows, columns, {}, None)

        fin = self.infile.clone()

        for col in rg.columns:
            name = col.meta_data.path_in_schema[0]
            if name in columns:
                off = min((col.meta_data.dictionary_page_offset or col.meta_data.data_page_offset,
                           col.meta_data.data_page_offset))
                fin.prefetch(off, col.meta_data.total_compressed_size + 65536)
                core.read_col(col, self.schema, fin, assign=assign[name])
                print(f'RG {index} Column {name} total_uncompressed_size {col.meta_data.total_uncompressed_size} total_compressed_size {col.meta_data.total_compressed_size}')

        fin.close()
        del fin
        return df


class ArrowParquetReader(pyarrow.parquet.ParquetFile):
    def __init__(self, path: str, storage_ip=None):
        self.url = urllib.parse.urlparse(path)
        if self.url.scheme == 'webhdfs':
            self.infile = WebHdfsFile(path, storage_ip=storage_ip)
        else:
            self.infile = open(self.url.path, 'rb')

        super(ArrowParquetReader, self).__init__(self.infile)
        self.columns = self.metadata.schema.names
        self.dtypes = [numpy.dtype(d.to_pandas_dtype()) for d in self.schema_arrow.types]
        # self.num_row_groups = self.metadata.num_row_groups
        self.infile.close()

    def read_rg(self, index: int, columns: list):
        if self.url.scheme == 'webhdfs':
            fin = self.infile.clone()
        else:
            fin = open(self.url.path, 'rb')

        pfin = pyarrow.parquet.ParquetFile(fin, metadata=self.metadata)
        df = pfin.read_row_group(index, columns=columns)
        fin.close()
        return df

def get_storage_ip():
    result = subprocess.run('docker network inspect qflock-net'.split(' '), stdout=subprocess.PIPE)
    d = json.loads(result.stdout)

    for c in d[0]['Containers'].values():
        print(c['Name'], c['IPv4Address'].split('/')[0])
        if c['Name'] == 'qflock-storage':
            return c['IPv4Address'].split('/')[0]


def get_webhdfs_reader():
    fname = '/tpcds-parquet/store_sales.parquet/part-00000-40849ae6-37a1-4e22-848d-d4cfea002b2b-c000.snappy.parquet'

    # storage_ip = get_storage_ip()
    # return get_reader(f'webhdfs://{storage_ip}:9870/{fname}?user.name=peter', storage_ip)
    storage_ip = '127.0.0.1'
    return get_reader(f'webhdfs://{storage_ip}:9860/{fname}?user.name=peter', storage_ip)



def get_file_reader():
    fname = '/mnt/usb-SanDisk_Ultra_128GB/caerus-dikeHDFS/data/tpch-test-parquet-1g/lineitem.parquet/' \
            'part-00000-badcef81-d816-44c1-b936-db91dae4c15f-c000.snappy.parquet'

    return get_reader(f'file://dikehdfs:9870/{fname}?user.name=peter')


def get_network_stats(docker_name):
    # docker stats --no-stream --format "{{.NetIO}}" qflock-storage
    cmd = 'docker stats --no-stream --format "{{.NetIO}}" ' + docker_name
    result = subprocess.run(cmd.split(' '), stdout=subprocess.PIPE)
    stats = result.stdout.decode("utf-8")
    stats = stats.lower().replace('\n', '').replace('"', '').replace(' ', '').replace('b', '').split('/')
    print(stats)
    sizes = {'k': 1 << 10, 'm': 1 << 20, 'g': 1 << 30}

    for i in range(0, len(stats)):
        factor = 1
        if stats[i][-1] in sizes.keys():
            factor = sizes[stats[i][-1]]
            stats[i] = stats[i][:-1]

        stats[i] = float(stats[i]) * factor

    return stats


if __name__ == '__main__':
    # reader = get_file_reader()
    reader = get_webhdfs_reader();

    dtypes = [t.name for t in reader.dtypes]

    total_compressed = 0

    for c in range(0, len(reader.columns)):
        c_name = reader.columns[c]
        c_type = dtypes[c]
        c_total_uncompressed = reader.info['rows'] * 8  # We need to check c_type here
        c_total_compressed = 0
        for i in range(0, reader.num_row_groups):
            c_total_compressed += reader.row_groups[i].columns[c].meta_data.total_compressed_size

        total_compressed += c_total_compressed
        c_ratio = c_total_uncompressed / c_total_compressed
        print(f'{c_name:<24} {c_type:<10} {c_total_uncompressed:<10} {c_total_compressed:<10} {c_ratio:<10.1f}')

    delta = (reader.infile.size - total_compressed) * 100 / reader.infile.size
    print(f'Total compressed {total_compressed} File size {reader.infile.size} Delta {delta:.3f}%')

    # Read one column with Docker network stats
    stats_before = get_network_stats('qflock-storage')
    for i in range(0, reader.num_row_groups):
        reader.read_rg(i, ['ss_sold_date_sk'])
    stats_after = get_network_stats('qflock-storage')
    transfer_size = int(stats_after[1] - stats_before[1])
    print(f'Transfer size {transfer_size}')




#! /usr/bin/python3
import sys
import time
import pyspark
from pyspark.sql.types import StringType, DoubleType, IntegerType, LongType, ShortType

import pyarrow
import pyarrow.parquet as pq
import zstandard as zstd
import pandas as pd
import numpy as np

class CompressTest:
    def __init__(self):
        self._spark = pyspark.sql.SparkSession \
            .builder \
            .appName("test") \
            .getOrCreate()
        self._compression = True
        self._tables = ["web_returns", "store_sales", "item", "call_center", "inventory"]
        self.create_tables()

    def create_tables(self):
        for table in self._tables:
            print(f"create table {table}")
            tablePath = f"hdfs://qflock-storage-dc2:9000/tpcds-parquet-100g/{table}.parquet"
            df0 = self._spark.read.format("parquet").load(tablePath)
            df0.createOrReplaceTempView(table)

    def compress(self, query):
        df = self._spark.sql(query)
        df_pandas = df.toPandas()
        num_rows = len(df_pandas.index)
        print(f"query toPandas() done rows:{num_rows}")
        df_schema = df.schema
        binary_rows = []
        col_type_bytes = []
        col_bytes = []
        comp_rows = []
        col_comp_bytes = []
        batch_size = int(1024 * 1024 * 2)
        total_rows = 0
        wrote = False
        if num_rows > 0:
            columns = df.columns
            for col_idx in range(0, len(columns)):
                # data = np_array[:,col_idx]
                data_np = df_pandas[columns[col_idx]].to_numpy()
                if batch_size == 0:
                    batches = 1
                else:
                    # batches = int(data_np.size / batch_size) + 1 if ((data_np.size % batch_size) > 0) else 0
                    batches = 16
                row_idx = 0
                batch_rows = [25450100, 25450100, 25450100, 25450100,
                              25450100, 25450100, 25450100, 25450100,
                              25450100, 25450100, 25450100, 25450100,
                              25470100, 25430100, 25430100, 17498500]
                for batch_idx in range(0, batches):
                    curr_batch_size = batch_rows[batch_idx] # batch_size
                    if batch_idx < batches - 1:
                        end_idx = row_idx + curr_batch_size
                        data = data_np[row_idx:end_idx]
                    else:
                        data = data_np[row_idx:]
                    num_rows = data.size
                    total_rows += num_rows
                    row_idx += curr_batch_size
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
                            ratio = num_bytes / len(new_data)
                            print(f"{col_name} rows:{num_rows} bytes: {num_bytes}:{len(new_data)} ratio: {ratio:.2f}")
                            raw_bytes = new_data
                            comp_rows.append(raw_bytes)
                        else:
                            raw_bytes = new_data.tobytes()
                            col_comp_bytes.append(len(raw_bytes))
                            binary_rows.append(raw_bytes)
                    else:
                        new_data = data.byteswap().newbyteorder().tobytes()
                        if not wrote:
                            with open(f"{col_name}_binary_data.bin", "wb") as fd:
                                fd.write(new_data)
                        # new_data = data.tobytes()
                        num_bytes = len(new_data)
                        col_bytes.append(num_bytes)
                        if self._compression is True:
                            # logging.debug(f"compressing col:{col_name} rows:{num_rows} bytes: {num_bytes}")
                            new_data = zstd.ZstdCompressor().compress(new_data)
                            if not wrote:
                                with open(f"{col_name}_compressed.bin", "wb") as fd:
                                    fd.write(new_data)
                                wrote = True
                            col_comp_bytes.append(len(new_data))
                            comp_rows.append(new_data)
                            ratio = num_bytes / len(new_data)
                            print(f"{col_name} rows {num_rows} uncompressed {num_bytes} compressed {len(new_data)} ratio {ratio:.2f}")
                        else:
                            col_comp_bytes.append(len(new_data))
                            binary_rows.append(new_data)
        uncomp_bytes = sum(col_bytes)
        comp_bytes = sum(col_comp_bytes)
        ratio = uncomp_bytes / comp_bytes
        print(f"rows {total_rows} uncomp {uncomp_bytes} comp {comp_bytes} {ratio:.2f}")

    def time_compress(self, query):
        start_time = time.time()
        self.compress(query)
        duration = time.time() - start_time
        print(f"query {query}")
        print(f"seconds {duration:.2f}")

if __name__ == "__main__":
  c = CompressTest()
  query = " ".join(sys.argv[1:])
  print(f"query {query}")
  c.time_compress(query)
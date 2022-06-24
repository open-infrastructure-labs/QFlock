#!/usr/bin/env python3
import sys
import os
from metastore_client import HiveMetastoreClient

import pyarrow.parquet
import pyarrow.fs

class TableMetadata:
    def __init__(self, dest_path=""):
        metastore_ip = "qflock-storage-dc1"
        metastore_port = "9084"
        self._metastore_client = HiveMetastoreClient(metastore_ip, metastore_port)
        self._dest_path = dest_path

    def get_file_stats(self, location: str):
        # Open parquet file
        fs, path = pyarrow.fs.FileSystem.from_uri(location)
        file_info = fs.get_file_info(pyarrow.fs.FileSelector(path))
        files = [f.path for f in file_info if f.is_file and f.size > 0]

        f = fs.open_input_file(files[0])
        reader = pyarrow.parquet.ParquetFile(f)
        num_rows = reader.metadata.num_rows
        num_row_groups = reader.num_row_groups
        f.close()
        return num_rows, num_row_groups

    def get_table_info(self):
        if not os.path.exists(self._dest_path):
            os.mkdir(self._dest_path)
        db_name = 'tpcds'
        table_names = self._metastore_client.client.get_all_tables(db_name)

        tables = [self._metastore_client.client.get_table(db_name, table_name)
                  for table_name in table_names]
        tables.sort(key=lambda tbl: int(tbl.sd.parameters['qflock.storage_size']), reverse=True)

        with open(os.path.join(self._dest_path, "tables.csv"), "w") as fd:
            print("table name,path,data center,bytes,metastore rows,metastore row groups," +
                  "parquet rows,parquet row groups", file=fd)
            for tbl in tables:
                dc = "dc1" if "dc1" in tbl.sd.location else "dc2"
                rows, row_groups = self.get_file_stats(tbl.sd.location)
                stat_name = f"spark.qflock.statistics.tableStats.{tbl.tableName}.row_groups"
                print(f"{tbl.tableName},{tbl.sd.location},{dc},{tbl.sd.parameters['qflock.storage_size']},"
                      f"{tbl.parameters['spark.sql.statistics.numRows']},"
                      f"{tbl.parameters[stat_name]},{rows},{row_groups}", file=fd)

if __name__ == '__main__':
    dest = ""
    if len(sys.argv) > 1:
        dest = sys.argv[1]
    tbl = TableMetadata(dest_path=dest)
    tbl.get_table_info()




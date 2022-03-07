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
import os
from pyspark.sql.types import StringType, LongType, DoubleType, StructType
from tpcds.tpcds_tables import tpcds_table_dict
from tpch.tpch_tables import tpch_table_dict

class TpcTables:
    def __init__(self, data):
        self._data = data

    def get_tables(self):
        return self._data.keys()

    @staticmethod
    def get_table_path(fpath, table_name):
        return os.path.join(fpath, table_name + ".parquet")

    def get_schema(self, table_name):
        schema = ",".join([f"{c['name']} {c['type']}" for c in self._data[table_name]['columns']])
        return schema

    def get_struct_type(self, table_name):
        struct_type = StructType()
        for f in self._data[table_name]['columns']:
            if f['type'] == "STRING":
                struct_type = struct_type.add(f['name'], StringType(), True)
            elif f['type'] == "LONG":
                struct_type = struct_type.add(f['name'], LongType(), True)
            elif f['type'] == "DOUBLE":
                struct_type = struct_type.add(f['name'], DoubleType(), True)
            else:
                raise KeyError(f"unexpected type {f['type']}")
        return struct_type


tpch_tables = TpcTables(tpch_table_dict)
tpcds_tables = TpcTables(tpcds_table_dict)

if __name__ == "__main__":
    print("="*80)
    t = tpch_tables.get_struct_type("customer")
    print(t)
    print("="*80)
    t = tpcds_tables.get_struct_type("customer")
    print(t)
    print("="*80)
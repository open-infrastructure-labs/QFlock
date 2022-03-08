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
import time
import re
import traceback
import pyspark
from benchmark.benchmark_result import BenchmarkResult


class SparkHelper:
    create_cmd_template = "CREATE EXTERNAL TABLE IF NOT EXISTS {}({}) "\
                          "USING PARQUET OPTIONS(path \"{}\");"
    drop_cmd_template = "DROP TABLE IF EXISTS {};"

    def __init__(self, app_name="test", catalog="hive", verbose=False):
        print(f"SparkHelper catalog: {catalog}")
        self._verbose = verbose
        self._spark = pyspark.sql.SparkSession\
            .builder\
            .appName(app_name)\
            .config("metastore.catalog.default", catalog)\
            .enableHiveSupport()\
            .getOrCreate()
        print(f"metastore.catalog.default: {self._spark.conf.get('metastore.catalog.default')}")

    def set_log_level(self, level="INFO"):
        self._spark.sparkContext.setLogLevel(level)

    def create_table(self, tables, table, db_path):
        create_cmd = \
            SparkHelper.create_cmd_template.format(table,
                                                   tables.get_schema(table),
                                                   tables.get_table_path(db_path, table))
        if self._verbose:
            print(create_cmd)
        self._spark.sql(create_cmd)

    def create_tables(self, tables, db_path):
        for t in tables.get_tables():
            print("create table for", t)
            self.create_table(tables, t, db_path)

    def get_catalog_info(self):
        databases = {}
        db_list = self._spark.catalog.listDatabases()
        print("*" * 50)
        print(f"found {len(db_list)} databases")
        print("*" * 50)

        for db in db_list:
            if self._verbose:
                self._spark.sql(f"DESCRIBE DATABASE EXTENDED {db.name}").show(5000, False)
            else:
                print(f"database: {db.name}")
            i = 0
            tables = {}
            tables_list = self._spark.catalog.listTables(db.name)
            print(f"found {len(tables_list)} tables")
            for tbl in tables_list:
                if self._verbose:
                    self._spark.sql(f"DESCRIBE TABLE EXTENDED {db.name}.{tbl.name}").show(5000, False)
                else:
                    print(f"  {i}) {tbl.database}.{tbl.name} {tbl.tableType}")
                i += 1
                c = 0
                tables[tbl.name] = {'columns': []}
                for col in self._spark.catalog.listColumns(tbl.name, db.name):
                    tables[tbl.name]['columns'].append({'name': col.name, 'type': col.dataType})
                    c += 1
            databases[db.name] = tables
            print("*" * 50)
        return databases

    def get_catalog_columns(self, column_filter=None):
        databases = {}
        db_list = self._spark.catalog.listDatabases()
        print("*" * 50)
        print(f"found {len(db_list)} databases")
        print("*" * 50)
        print(f"tbl {column_filter}")
        tbl_filter = None
        col_filter = None
        if column_filter != "*":
            items = column_filter.split(".")
            if len(items) == 1:
                col_filter = items[0]
            if len(items) == 2:
                if items[0] != "*":
                    tbl_filter = items[0]
                if items[1] != "*":
                    col_filter = items[1]
        for db in db_list:
            print(f"database: {db.name}")
            i = 0
            tables_list = self._spark.catalog.listTables(db.name)
            print(f"found {len(tables_list)} tables")
            for tbl in tables_list:
                if tbl_filter is None or tbl_filter in tbl:
                    print(f"  {i}) {tbl.database}.{tbl.name} {tbl.tableType}")
                    i += 1
                    c = 0
                    for col in self._spark.catalog.listColumns(tbl.name, db.name):
                        if col_filter is None or col_filter in col.name:
                            if self._verbose:
                                self._spark.sql(f"DESCRIBE EXTENDED " +
                                                f"{db.name}.{tbl.name} {col.name}")\
                                    .show(5000, False)
                            else:
                                print(f"    {c}) {col.name} {col.dataType}")
                        c += 1
            print("*" * 50)
        return databases

    def drop_table(self, name):
        drop_cmd = SparkHelper.drop_cmd_template.format(name)
        print(drop_cmd)
        self._spark.sql(drop_cmd)

    def delete_tables(self):
        db = self.get_catalog_info()
        for d in db:
            for t in db[d].keys():
                self.drop_table(t)

    def query(self, query, explain=False, query_name=""):
        start_time = time.time()
        status = 0
        df = None
        explain_plan = None
        try:
            if explain:
                df = self._spark.sql(f"explain cost {query}")
                explain_plan = df.collect()[0]['plan']
            else:
                df = self._spark.sql(query)
                df.collect()
        except (ValueError, Exception):
            print(f"caught error executing query for {query}")
            print(traceback.format_exc())
            status = 1
        duration = time.time() - start_time
        if self._verbose:
            print(df)
        return BenchmarkResult(df, status=status, duration_sec=duration, explain_text=explain_plan,
                               verbose=self._verbose, explain=explain, query_name=query_name)

    def query_from_file(self, query_file, explain=False):
        with open(query_file, "r") as fd:
            lines = []
            for line in fd.readlines():
                new_line = re.sub("^(.*)--(.*)$", "", line).replace("\n", "")
                new_line = re.sub("\\s+", " ", new_line)
                lines.append(new_line)
            query = " ".join(lines)
            if self._verbose:
                print(f"Executing spark query {query_file}: {query}")
            result = self.query(query, explain, query_name=query_file)
        return result

    @staticmethod
    def write_my_sql(df):
        df.write.format('jdbc').options(
            url='jdbc:mysql://172.19.0.2/tpch',
            driver='com.mysql.jdbc.Driver',
            dbtable='region',
            user='root',
            password='my-secret-pw').mode('append').save()

    def write_file_as_parquet(self, schema, input_file, output_file):
        print("write_file_as_parquet")
        print(f"schema {schema}")
        print(f"input_file {input_file}")
        df = self._spark.read.options(delimiter='|').schema(schema).csv(input_file)
        print(f"database {input_file} has {df.count()} rows")

        df.repartition(1) \
            .write \
            .option("header", True) \
            .option("partitions", "1") \
            .format("parquet") \
            .save(output_file)

    def set_db(self, name):
        self._spark.sql(f"USE {name}")

    def create_db(self, name):
        self._spark.sql(f"CREATE DATABASE IF NOT EXISTS {name}")

    def show_db(self, name):
        self._spark.sql(f"DESCRIBE DATABASE EXTENDED {name}")

# spark = SparkSession.\
#     builder\
#     .appName("test")\
#     .config("spark.sql.extensions","com.github.datasource.generic.FederationExtensions")\
#     .getOrCreate()

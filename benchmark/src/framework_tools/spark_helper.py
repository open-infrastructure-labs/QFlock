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
import time
import re
import traceback
import pyspark
from benchmark.benchmark_result import BenchmarkResult


class SparkHelper:
    create_cmd_template = "CREATE EXTERNAL TABLE IF NOT EXISTS {}({}) "\
                          "USING PARQUET OPTIONS(path \"{}\");"
    drop_cmd_template = "DROP TABLE IF EXISTS {};"

    def __init__(self, app_name="test", use_catalog=False, verbose=False,
                 jdbc=None, qflock_ds=False, output_path=None):
        self._verbose = verbose
        self._jdbc = jdbc
        self._qflock_ds = qflock_ds
        self._output_path = output_path
        if self._jdbc or self._qflock_ds:
            use_catalog = False
        if use_catalog:
            self._spark = pyspark.sql.SparkSession\
                .builder\
                .appName(app_name)\
                .config("qflockJdbcUrl", self._jdbc)\
                .enableHiveSupport()\
                .getOrCreate()
        else:
            self._spark = pyspark.sql.SparkSession\
                .builder\
                .appName(app_name)\
                .config("qflockJdbcUrl", self._jdbc)\
                .getOrCreate()

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

    def load_extension(self):
        from py4j.java_gateway import java_import
        gw = self._spark.sparkContext._gateway
        java_import(gw.jvm, "com.github.qflock.extensions.QflockJdbcDialect")
        gw.jvm.org.apache.spark.sql.jdbc.JdbcDialects.registerDialect(
            gw.jvm.com.github.qflock.extensions.QflockJdbcDialect())

    def load_rule(self, ext):
        from py4j.java_gateway import java_import
        gw = self._spark.sparkContext._gateway
        if ext == "explain":
            # print("Loading explain rule")
            java_import(gw.jvm, "com.github.qflock.extensions.rules.QflockExplainRuleBuilder")
            gw.jvm.com.github.qflock.extensions.rules.QflockExplainRuleBuilder.injectExtraOptimization()
        elif ext == "jdbc":
            # print("Loading jdbc rule")
            java_import(gw.jvm, "com.github.qflock.extensions.rules.QflockRuleBuilder")
            gw.jvm.com.github.qflock.extensions.rules.QflockRuleBuilder.injectExtraOptimization()

    def create_table_view(self, table, db_path, db_name):
        if self._jdbc:
            df = self._spark.read.option("url", db_path)\
                 .option("batchSize", "100000")\
                 .format("jdbc")\
                 .option("header", "true") \
                 .option("driver", "com.github.qflock.jdbc.QflockDriver") \
                 .option("dbtable", table).load()
            df.createOrReplaceTempView(table)
        elif self._qflock_ds:
            table_path = os.path.join(db_path, f"{table}.parquet")
            # The table path is something like: hdfs://server/db_dir/table_dir
            df = self._spark.read\
                .format("qflockDs") \
                .option("format", "parquet") \
                .option("tableName", table) \
                .option("dbName", db_name) \
                .load()
            df.createOrReplaceTempView(table)
        else:
            table_path = os.path.join(db_path, f"{table}.parquet")
            df = self._spark.read.parquet(table_path)
            df.createOrReplaceTempView(table)

    def create_tables_view(self, tables, db_path, db_name):
        for t in tables.get_tables():
            print("create temp view table for", t)
            self.create_table_view(t, db_path, db_name)

    def get_catalog_info(self):
        databases = {}
        db_list = self._spark.catalog.listDatabases()
        print("*" * 50)
        print(f"found {len(db_list)} databases")
        print("*" * 50)

        for db in db_list:
            self._spark.sql(f"USE {db.name}")
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
                    rows = self._spark.sql(f"select * from {tbl.name}").count()
                    print(f"  {i}) {tbl.database}.{tbl.name} {tbl.tableType} {rows}")
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
        rows = []
        new_df = None
        try:
            if explain:
                df = self._spark.sql(f"explain cost {query}")
                explain_plan = df.collect()[0]['plan']
            else:
                df = self._spark.sql(query)
                df_rows = df.collect()
                rows = df_rows
        except (ValueError, Exception):
            print(f"caught error executing query for {query}")
            print(traceback.format_exc())
            status = 1
        duration = time.time() - start_time
        
        # Make local df from rows to avoid re-evaluation of df if we want to write it.
        if not explain:
            new_df = self._spark.createDataFrame(data=rows, schema=df.schema)
        
        return BenchmarkResult(new_df, status=status, duration_sec=duration, explain_text=explain_plan,
                               verbose=self._verbose, explain=explain, query_name=query_name,
                               output_path=self._output_path, num_rows=len(rows))

    def query_from_file(self, query_file, explain=False, limit=None):
        with open(query_file, "r") as fd:
            lines = []
            for line in fd.readlines():
                new_line = re.sub("^(.*)--(.*)$", "", line).replace("\n", "")
                new_line = re.sub("\\s+", " ", new_line)
                lines.append(new_line)
            query = " ".join(lines)
            if limit and 'LIMIT' not in query:
                query += f" LIMIT {limit}"
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
        if self._verbose:
            print("write_file_as_parquet")
            print(f"schema {schema}")
            print(f"input_file {input_file}")
        df = self._spark.read.options(delimiter='|').schema(schema).csv(input_file)
        print(f"database {input_file} has {df.count()} rows")

        #df.repartition(1) \
        df.repartition(1).fillna(0).fillna("") \
            .write \
            .option("header", True) \
            .option("partitions", "1") \
            .format("parquet") \
            .save(output_file)

    def set_db(self, name):
        self._spark.sql(f"USE {name}")

    def create_db(self, name):
        print(f"creating database {name}")
        self._spark.sql(f"CREATE DATABASE IF NOT EXISTS {name}")

    def delete_db(self, name):
        print(f"deleting database {name}")
        self._spark.sql(f"DROP DATABASE IF EXISTS {name}")

    def show_db(self, name):
        self._spark.sql(f"DESCRIBE DATABASE EXTENDED {name}")

# spark = SparkSession.\
#     builder\
#     .appName("test")\
#     .config("spark.sql.extensions","com.github.datasource.generic.FederationExtensions")\
#     .getOrCreate()

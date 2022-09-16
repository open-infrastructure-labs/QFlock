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
import glob
import time
import logging
import traceback

from pathlib import Path
from pyspark.sql import functions as func
from pyspark.sql.types import DoubleType


class BenchmarkResult:
    log_dir = "logs"

    def __init__(self, df, status=0, query_start_time=0,
                 verbose=False, explain=False, query_name=None, output_path=None,
                 num_rows=None, spark_helper=None, query=None,
                 save_data=False, overall_start_time=None):
        self.df = df
        self.explain_text = ""
        self.status = status
        self.duration_sec = 0
        self._query_start_time = query_start_time
        self._overall_start_time = overall_start_time
        self._save_data = save_data
        self.num_rows = num_rows
        self._collect_rows = None
        self._verbose = verbose
        self._explain = explain
        self._explain_plan = None
        self.query_name = query_name
        self._spark_helper = spark_helper
        self._query = query
        self.size_bytes_csv = 0
        self.size_bytes_pq = 0
        self.duration_sec = 0
        self.overall_duration_sec = 0
        if self._verbose:
            self._output = ["csv"]
            # self._output = ["parquet", "csv"]
        else:
            self._output = ["csv"]
        if not output_path:
            self._output_path = os.path.join(BenchmarkResult.log_dir, "output")
            self._save_data = False
        else:
            self._save_data = True
            self._output_path = output_path
        if query_name:
            qname = os.path.split(query_name)[1]
            self._output_path = os.path.join(self._output_path, qname)
            logging.info(f"output path: {self._output_path} ")

    def filtered_explain(self):
        new_explain_text = ""
        for line in self.explain_text.splitlines():
        #     if "== Physical Plan ==" in line:
        #         break
        #     elif "== Optimized Logical Plan" not in line:
            new_explain_text += line + "\n"
        return new_explain_text

    def formatted_df(self):
        df1 = self.df
        df1 = df1.orderBy(df1.columns, ascending=True)
        for c in range(0, len(df1.columns)):
            if isinstance(df1.schema.fields[c].dataType, DoubleType):
                df1 = df1.withColumn(df1.schema.fields[c].name,
                                     func.format_number(func.bround(df1[df1.schema.fields[c].name], 3), 2))
        return df1

    def process_result(self, collect_only):
        # if self._verbose:
        #     self.df.show(100, False)
        if collect_only:
            self._collect_rows = self.df.collect()
        else:
            root_path = os.path.split(self._output_path)[0]
            if not os.path.exists(root_path):
                logging.info(f"creating {root_path}")
            Path(root_path).mkdir(parents=True, exist_ok=True)
            if self._explain:
                self.explain_text = self.df.collect()[0]['plan']
                with open("logs/explain.txt", "a") as fd:
                    if self.query_name:
                        print(f"query: {self.query_name}", file=fd)
                    print(self.filtered_explain(), file=fd)
            elif "csv" in self._output:
                output_path = self._output_path + ".csv"
                logging.info(f"csv output path is {output_path}")
                if self._save_data:
                    logging.info(f"saving formatted output")
                    self.formatted_df().coalesce(1) \
                        .write.mode("overwrite") \
                        .format("csv") \
                        .option("header", "true") \
                        .option("partitions", "1") \
                        .save(output_path)
                elif True:
                    logging.info(f"saving unformatted output")
                    self.df \
                        .write.mode("overwrite") \
                        .format("csv") \
                        .option("header", "true") \
                        .save(output_path)
                else:
                    logging.info(f"not saving output")
                    rows = self.df.collect()
                    self.num_rows = len(rows)
                if self.num_rows is None:
                    output_files = glob.glob(os.path.join(output_path, 'part-*'))
                    if len(output_files) > 0:
                        self.size_bytes_csv = os.path.getsize(output_files[0])
                    else:
                        self.size_bytes_csv = os.path.getsize(output_path)
                    with open(output_files[0], 'r') as fp:
                        self.num_rows = len(fp.readlines())

            elif "parquet" in self._output:
                output_path = self._output_path + ".parquet"
                self.df.repartition(1) \
                    .write.mode("overwrite") \
                    .format("parquet") \
                    .option("header", "true") \
                    .option("partitions", "1") \
                    .save(output_path)
                output_files = glob.glob(os.path.join(output_path, 'part-*'))
                if len(output_files) > 0:
                    self.size_bytes_pq = os.path.getsize(output_files[0])
                else:
                    self.size_bytes_pq = os.path.getsize(output_path)
            self.duration_sec = time.time() - self._query_start_time
            self.overall_duration_sec = time.time() - self._overall_start_time

    def header(self):
        if self._verbose:
            header = f"query,rows,bytes,seconds"
        else:
            header = f"query,rows,seconds"
        return header

    def brief_result(self):
        if self._verbose:
            result = f"{self.query_name},{self.num_rows},"\
                     f"{self.size_bytes_pq},{self.duration_sec:.3f}"
        else:
            result = f"{self.query_name},{self.num_rows},"\
                     f"{self.duration_sec:.3f}"
        return result

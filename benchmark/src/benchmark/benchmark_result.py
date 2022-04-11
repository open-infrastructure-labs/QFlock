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
from pathlib import Path
from pyspark.sql import functions as func
from pyspark.sql.types import DoubleType


class BenchmarkResult:
    log_dir = "logs"

    def __init__(self, df, status=0, duration_sec=0, explain_text="",
                 verbose=False, explain=False, query_name=None, output_path=None,
                 num_rows=None):
        self.df = df
        self.explain_text = explain_text
        self.status = status
        self.duration_sec = duration_sec
        self.num_rows = num_rows
        self._verbose = verbose
        self._explain = explain
        self.query_name = query_name
        self.size_bytes_csv = 0
        self.size_bytes_pq = 0
        if self._verbose:
            self._output = ["csv"]
            # self._output = ["parquet", "csv"]
        else:
            self._output = []
        if not output_path:
            self._output_path = os.path.join(BenchmarkResult.log_dir, "output")
        else:
            self._output_path = output_path
        if query_name:
            qname = os.path.split(query_name)[1]
            self._output_path = os.path.join(self._output_path, qname)
            print(f"output path: {self._output_path} ")

    def filtered_explain(self):
        new_explain_text = ""
        for line in self.explain_text.splitlines():
            if "== Physical Plan ==" in line:
                break
            elif "== Optimized Logical Plan" not in line:
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

    def process_result(self):
        if self._verbose:
            if self._explain:
                print(self.explain_text)
            # else:
            #     self.df.show(100, False)
        root_path = os.path.split(self._output_path)[0]
        if not os.path.exists(root_path):
            print(f"creating {root_path}")
            Path(root_path).mkdir(parents=True, exist_ok=True)
        if self._explain:
            with open("logs/explain.txt", "a") as fd:
                if self.query_name:
                    print(f"query: {self.query_name}", file=fd)
                print(self.filtered_explain(), file=fd)
        if "csv" in self._output:
            output_path = self._output_path + ".csv"
            print(f"csv output path is {output_path}")
            self.formatted_df().repartition(1) \
                .write.mode("overwrite") \
                .format("csv") \
                .option("header", "true") \
                .option("partitions", "1") \
                .save(output_path)
            output_files = glob.glob(os.path.join(output_path, 'part-*'))
            if len(output_files) > 0:
                self.size_bytes_csv = os.path.getsize(output_files[0])
            else:
                self.size_bytes_csv = os.path.getsize(output_path)
        if "parquet" in self._output:
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

    def brief_result(self):
        if self._verbose:
            result = f"qflock:: {self.query_name} rows {self.num_rows} "\
                     f"bytes {self.size_bytes_pq} seconds {self.duration_sec:.3f}"
        else:
            result = f"qflock:: {self.query_name} rows {self.num_rows} "\
                     f"seconds {self.duration_sec:.3f}"
        return result

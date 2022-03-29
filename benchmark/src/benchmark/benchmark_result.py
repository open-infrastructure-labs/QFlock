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


class BenchmarkResult:
    log_dir = "logs"

    def __init__(self, df, status=0, duration_sec=0, explain_text="",
                 verbose=False, explain=False, query_name=None):
        self.df = df
        self.explain_text = explain_text
        self.status = status
        self.duration_sec = duration_sec
        self._verbose = verbose
        self._explain = explain
        self.query_name = query_name
        self.size_bytes_csv = 0
        self.size_bytes_pq = 0
        if self._verbose:
            self._output = ["parquet", "csv"]
        else:
            self._output = []

    def filtered_explain(self):
        new_explain_text = ""
        for line in self.explain_text.splitlines():
            if "== Physical Plan ==" in line:
                break
            elif "== Optimized Logical Plan" not in line:
                new_explain_text += line + "\n"
        return new_explain_text

    def process_result(self):
        if self._verbose:
            if self._explain:
                print(self.explain_text)
            # else:
            #     self.df.show(100, False)
        if self._explain:
            if not os.path.exists(BenchmarkResult.log_dir):
                os.mkdir(BenchmarkResult.log_dir)
            with open("logs/explain.txt", "a") as fd:
                if self.query_name:
                    print(f"query: {self.query_name}", file=fd)
                print(self.filtered_explain(), file=fd)
        if "csv" in self._output:
            output_path = os.path.join("logs", "output.csv")
            self.df.repartition(1) \
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
            output_path = os.path.join("logs", "output.parquet")
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
            result = f"qflock:: {self.query_name} rows {self.df.count()} "\
                     f"bytes {self.size_bytes_pq} seconds {self.duration_sec:.3f}"
        else:
            result = f"qflock:: {self.query_name} "\
                     f"seconds {self.duration_sec:.3f}"
        return result

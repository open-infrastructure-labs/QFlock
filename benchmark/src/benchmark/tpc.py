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
import shutil
import time
from glob import glob

from benchmark.command import shell_cmd
from benchmark.benchmark import Benchmark
from benchmark.tpc_tables import tpch_tables
from benchmark.tpc_tables import tpcds_tables
from benchmark.docker_stat import DockerStat
from benchmark.hdfs_log_stat import HdfsLogStat


class TpcBenchmark(Benchmark):
    """A TPC benchmark, which is capable of generating the tables, and
       running queries against those tables."""

    def __init__(self, name, config, framework, tables, verbose=False, catalog=True,
                 jdbc=False):
        super().__init__(name, config, framework)
        self._tables = tables
        self._file_ext = "." + self._config['file-extension']
        self._verbose = verbose
        self._catalog = catalog
        self._jdbc = jdbc
        if self._jdbc:
            self._catalog = False
        self._stat_list = [HdfsLogStat()]
        self._stat_list.extend(DockerStat.get_stats(self._config['docker-stats']))

    def generate(self):
        raw_base_path = ""
        for p in os.path.split(self._config['raw-data-path']):
            raw_base_path = os.path.join(raw_base_path, p)
            if not os.path.exists(raw_base_path):
                os.mkdir(raw_base_path)
        cwd = os.getcwd()
        os.chdir(self._config['tool-path'])
        shell_cmd(self._config['tool-commandline'])
        os.chdir(cwd)
        files = os.listdir(self._config['tool-path'])
        filtered = [f for f in files if self._file_ext in f]
        file_count = 0
        for f in filtered:
            file_path = os.path.join(self._config['tool-path'], f)
            dest_path = os.path.join(self._config['raw-data-path'], f)
            if os.path.isfile(file_path) and self._file_ext in f:
                if os.path.exists(dest_path):
                    os.remove(dest_path)
                shutil.move(file_path, dest_path)
                file_count += 1
        print(f"{file_count} files copied {self._config['tool-path']} -> "
              f"{self._config['raw-data-path']}")

    def query_text(self, query_string, explain=False):
        if self._catalog:
            self._framework.set_db(self._config['db-name'])
        for s in self._stat_list:
            s.start()
        print("qflock::starting query::")
        result = self._framework.query(query_string, explain=explain)
        print("qflock::query finished::")
        stat_result = ""
        for s in self._stat_list:
            s.end()
            stat_result += str(s)

        print("qflock::process result::")
        if result is not None:
            result.process_result()
            print("qflock::process result done::")
        print(result.brief_result() + " " + stat_result)
        return result

    def query_file(self, query_file, explain=False):
        if self._catalog:
            self._framework.set_db(self._config['db-name'])
        for s in self._stat_list:
            s.start()
        print("qflock::starting query::")
        result = self._framework.query_from_file(query_file, explain=explain)
        print("qflock::query finished::")
        stat_result = ""
        for s in self._stat_list:
            s.end()
            stat_result += str(s)
        print("qflock::process result::")
        if result is not None:
            result.process_result()
            print("qflock::process result done::")
        print(result.brief_result() + " " + stat_result)
        return result

    def query_range(self, query_config, explain=False):
        if self._catalog:
            self._framework.set_db(self._config['db-name'])
        query_list = Benchmark.get_query_list(query_config['query_range'], self._config['query-path'],
                                              self._config['query-extension'])
        success_count, failure_count = 0, 0
        if self._verbose:
            print(f"query_list {query_list}")
        print(f"query_range {query_config['query_range']}")

        for q in query_list:
            result = self.query_file(q, explain)
            if result.status == 0:
                success_count += 1
            else:
                failure_count += 1

        print(f"SUCCESS: {success_count} FAILURE: {failure_count}")

    def write_parquet(self, base_input_path, base_output_path):
        for table in self._tables.get_tables():
            full_output_path = base_output_path + os.path.sep + table + ".parquet"
            full_input_path = base_input_path + os.path.sep + table + self._file_ext
            if not os.path.exists(full_input_path):
                print(f"input file {full_input_path} does not exist")
                exit(1)
            if not os.path.exists(full_output_path):
                self._framework.write_file_as_parquet(self._tables.get_struct_type(table),
                                                      full_input_path,
                                                      full_output_path)
            else:
                print(f"directory {full_output_path} already exists")

    def create_catalog(self):
        files_path = self._config['parquet-path']
        if 'hdfs' not in files_path:
            files_path = os.path.abspath(self._config['parquet-path'])
        print(f"creating catalog for {files_path}")
        self._framework.create_db(self._config['db-name'])
        self._framework.set_db(self._config['db-name'])
        self._framework.create_tables(self._tables, files_path)

    def create_tables_view(self):
        if self._jdbc:
            db_path = self._config['jdbc-path']
        else:
            db_path = self._config['parquet-path']
            if 'hdfs' not in db_path:
                db_path = os.path.abspath(self._config['parquet-path'])
        if self._verbose:
            print(f"qflock::creating table view for {db_path}")
        self._framework.create_tables_view(self._tables, db_path)

    def compute_stats(self):
        for table in self._tables.get_tables():
            print(f"computing stats for table {table}")
            self._framework.query(f"analyze table {table} COMPUTE STATISTICS FOR ALL COLUMNS")


class TpchBenchmark(TpcBenchmark):
    """A TPC-H benchmark, which is capable of generating the TPC-H tables, and
       running queries against those tables."""

    def __init__(self, config, framework, verbose=False, catalog=True, jdbc=False):
        super().__init__("TPC-H", config, framework, tpch_tables, verbose, catalog, jdbc)

class TpcdsBenchmark(TpcBenchmark):
    """A TPC-DS benchmark, which is capable of generating the TPC-DS tables, and
       running queries against those tables."""

    def __init__(self, config, framework, verbose=False, catalog=True, jdbc=False):
        super().__init__("TPC-DS", config, framework, tpcds_tables, verbose, catalog, jdbc)

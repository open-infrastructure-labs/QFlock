#!/usr/bin/python3
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
import argparse
from argparse import RawTextHelpFormatter

from pyfiglet import Figlet
import yaml

from benchmark.tpc import TpchBenchmark
from benchmark.tpc import TpcdsBenchmark
from framework_tools.spark_helper import SparkHelper


class BenchmarkApp:
    """Application for running benchmarks."""
    def __init__(self):
        self._args = None
        self._config = None

    def _load_config(self):
        config_file = self._args.file
        if not os.path.exists(config_file):
            print(f"{config_file} is missing.  Please add it.")
            exit(1)
        with open(config_file, "r") as fd:
            try:
                self._config = yaml.safe_load(fd)
            except yaml.YAMLError as err:
                print(err)
                print(f"{config_file} is missing.  Please add it.")
                exit(1)

    def get_parser(self, parent_parser=False):
        parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter,
                                         description="Application for running benchmarks.\n",
                                         add_help=(not parent_parser))
        if not parent_parser:
            parser.add_argument("--debug", "-D", action="store_true",
                                help="enable debug output")
            parser.add_argument("--verbose", "-v", action="store_true",
                                help="Increase verbosity of output.")
            parser.add_argument("--log_level", "-ll", default="ERROR",
                                help="log level set to input arg.  \n"
                                     "Valid values are ERROR, WARN, INFO, DEBUG, TRACE")
            parser.add_argument("--file", "-f", default="config.yaml",
                                help="config .yaml file to use")
            parser.add_argument("--query_text", "-qt", default=None,
                                help="run custom query against table")
            parser.add_argument("--query_range", "-qr", default=None,
                                help="run query number. \n"
                                     "e.g. * or 1,2,3-5,6-10,22,23-38")
            parser.add_argument("--view_columns", "-vcc", default=None,
                                help="View details of catalog column. example: table.column \n" +
                                     " or column. Example web_site.web_city or web_city")
        parser.add_argument("--init_all", action="store_true",
                            help="Equivalent to --gen_data, --gen_parquet, \n"
                                 "--create_catalog, --compute_stats, --view_catalog")
        parser.add_argument("--gen_data", "-g", action="store_true",
                            help="Generate raw data files for benchmark.")
        parser.add_argument("--gen_parquet", "-gp", action="store_true",
                            help="Generate parquet files from raw data files.")
        parser.add_argument("--view_catalog", "-vca", action="store_true",
                            help="View details of catalog")
        parser.add_argument("--delete_catalog", "-dc", action="store_true",
                            help="Delete catalog entries.")
        parser.add_argument("--create_catalog", "-cc", action="store_true",
                            help="Create the catalog database and tables for the parquet files.")
        parser.add_argument("--compute_stats", "-cs", action="store_true",
                            help="Compute statistics, including histograms (if enabled).")
        parser.add_argument("--explain", "-e", action="store_true",
                            help="For query commands, do explain instead of query.")
        parser.add_argument("--catalog", "-cat", default="hive",
                            help="catalog to use")
        parser.add_argument("--query_file", "-qf", default=None,
                            help="read query from file.")
        return parser

    def _parse_args(self):
        self._args = self.get_parser().parse_args()
        return True

    @staticmethod
    def _banner():
        print()
        f = Figlet(font='slant')
        print(f.renderText('QFlock Bench'))

    def _get_benchmark(self, sh):
        if self._config['benchmark']['db-name'] == "tpch":
            return TpchBenchmark(self._config['benchmark'], sh, self._args.verbose)
        if self._config['benchmark']['db-name'] == "tpcds":
            return TpcdsBenchmark(self._config['benchmark'], sh, self._args.verbose)
        return None

    def _get_query_config(self):
        qc = {}
        args = ["query_range"]
        for arg in args:
            if arg in self._args.__dict__:
                qc[arg] = self._args.__dict__[arg]
        return qc

    def trace(self, message):
        print("*" * 80)
        print(message)
        print("*" * 80)

    def run(self):
        if not self._parse_args():
            return
        self._load_config()
        sh = SparkHelper(catalog=self._args.catalog, verbose=self._args.verbose)
        # This trace is important
        # the calling {} will look for this before starting tracing.
        print("bench.py starting")
        if self._args.log_level:
            print(f"Set log level to {self._args.log_level}")
            sh.set_log_level(self._args.log_level)
            # sh.set_log_level("DEBUG")
        benchmark = self._get_benchmark(sh)
        BenchmarkApp._banner()
        if self._args.init_all:
            self._args.gen_data = True
            self._args.gen_parquet = True
            self._args.create_catalog = True
            self._args.compute_stats = True
            self._args.view_catalog = True
        if self._args.gen_data:
            self.trace("Starting to generate {} data".format(self._config['benchmark']['name']))
            benchmark.generate()
            self.trace("Generate {} data Complete.".format(self._config['benchmark']['name']))
        if self._args.gen_parquet:
            self.trace("Starting to generate {} parquet".format(self._config['benchmark']['name']))
            benchmark.write_parquet(self._config['benchmark']['raw-data-path'],
                                    self._config['benchmark']['parquet-path'])
            self.trace("Generate {} parquet Complete.".format(self._config['benchmark']['name']))
        if self._args.create_catalog:
            self.trace("Starting to create {} catalog".format(self._config['benchmark']['name']))
            benchmark.create_catalog()
            self.trace("Create {} catalog Complete".format(self._config['benchmark']['name']))
        if self._args.compute_stats:
            self.trace("Starting to compute {} stats".format(self._config['benchmark']['name']))
            sh.set_db(self._config['benchmark']['db-name'])
            benchmark.compute_stats()
            self.trace("Compute {} stats Complete".format(self._config['benchmark']['name']))
        if self._args.view_catalog:
            td = sh.get_catalog_info()
        if self._args.view_columns:
            td = sh.get_catalog_columns(self._args.view_columns)
        if self._args.query_text or self._args.query_file or self._args.query_range:
            result = None
            if self._args.query_text:
                sh.set_db(self._config['benchmark']['db-name'])
                print("Spark query", self._args.query_text)
                result = sh.query(self._args.query_text, self._args.explain)
                if result != None:
                    result.process_result()
                    print(result.brief_result())
            elif self._args.query_file:
                sh.set_db(self._config['benchmark']['db-name'])
                result = sh.query_from_file(self._args.query_file, self._args.explain)
                if result != None:
                    result.process_result()
                    print(result.brief_result())
            elif self._args.query_range:
                qc = self._get_query_config()
                result = benchmark.query(qc, self._args.explain)
            if self._args.explain:
                print("see logs/explain.txt for output of explain")


if __name__ == "__main__":
    bench = BenchmarkApp()
    bench.run()


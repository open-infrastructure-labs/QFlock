#!/usr/bin/env python3
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
import argparse
from argparse import RawTextHelpFormatter
import logging

from pyfiglet import Figlet
import yaml
import socket

from benchmark.benchmark_factory import BenchmarkFactory
from benchmark.metastore import MetastoreClient
from framework_tools.spark_helper import SparkHelper
from benchmark.config import Config
from benchmark.bench_logging import setup_logger


class BenchmarkApp:
    """Application for running benchmarks."""
    def __init__(self):
        self._args = None
        self._config = None
        self._start_time = time.time()

    def _load_config(self):
        config_file = self._args.file
        if not os.path.exists(config_file):
            logging.info(f"{config_file} is missing.  Please add it.")
            exit(1)
        with open(config_file, "r") as fd:
            try:
                self._config = yaml.safe_load(fd)
            except yaml.YAMLError as err:
                logging.info(err)
                logging.info(f"{config_file} is missing.  Please add it.")
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
            parser.add_argument("--output_path", default=None,
                                help="Prefix of Path for output files. " +
                                     "(.csv and/or .parquet is added to folder name)")
            parser.add_argument("--test_num", default=0, type=int,
                                help="The index of the test")
            parser.add_argument("--qflock_ds", action="store_true",
                                help="Use qflock parquet datasource")
            parser.add_argument("--results_path", default="data/",
                                help="directory for perf results.")
            parser.add_argument("--results_file", default=None,
                                help="file for perf results.")
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
        parser.add_argument("--no_catalog", action="store_true",
                            help="Disable use of catalog")
        parser.add_argument("--create_catalog", "-cc", action="store_true",
                            help="Create the catalog database and tables for the parquet files.")
        parser.add_argument("--compute_stats", "-cs", action="store_true",
                            help="Compute statistics, including histograms (if enabled).")
        parser.add_argument("--explain", "-e", action="store_true",
                            help="For query commands, do explain instead of query.")
        parser.add_argument("--jdbc", action="store_true",
                            help="Issue query to jdbc api of Spark.")
        parser.add_argument("--query_file", "-qf", default=None,
                            help="read query from file.")
        parser.add_argument("--loops", type=int, default=1,
                            help="number of times to loop the range of tests.")
        parser.add_argument("--capture_log_level", default=None,
                            help="log level to capture to file.")
        parser.add_argument("--continue_on_error", action="store_true",
                            help="continue with remaining queries if query encounters an error.")
        parser.add_argument("--ext", default=None,
                            help="Extension to load")
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
        return BenchmarkFactory.get_benchmark(self._config,
                                              sh,
                                              self._args.verbose,
                                              not self._args.no_catalog,
                                              self._args.jdbc,
                                              self._args.qflock_ds,
                                              self._args.test_num,
                                              results_file=self._args.results_file,
                                              results_path=self._args.results_path)

    def _get_query_config(self):
        qc = {}
        args = ["query_range", "query_file", "continue_on_error"]
        for arg in args:
            if arg in self._args.__dict__:
                qc[arg] = self._args.__dict__[arg]
        return qc

    def trace(self, message):
        if self._args.verbose or self._args.log_level != "OFF":
            print("*" * 50)
            print(message)
            print("*" * 50)
        else:
            print()
            print(message)

    def _create_default_catalog(self):
        """If the default catalog(s) are not present, then create them."""
        metastore_port = Config.get_metadata_ports(self._config['spark'])
        if 'hive-metastore' in self._config['benchmark']:
            hive_metastore = self._config['spark']['hive-metastore']
            if any(char.isalpha() for char in hive_metastore):
                hive_metastore = socket.gethostbyname(hive_metastore)
            mclient = MetastoreClient(hive_metastore,
                                      metastore_port['default'])
            catalogs = mclient.client.get_catalogs()
            for catalog_name in ["spark_dc"]:
                if self._args.verbose:
                    logging.info(f"qflock::found catalogs {catalogs}")
                if catalog_name not in catalogs.names:
                    logging.info(f"qflock::creating catalog {catalog_name}")
                    mclient.create_catalog(name=catalog_name, description='Spark Catalog for a Data Center',
                                           locationUri='/opt/volume/metastore/metastore_db_DBA')

    def run(self):
        print("bench.py starting")
        setup_logger()
        if not self._parse_args():
            return
        self._load_config()
        jdbc_config = None
        server_path = None
        if self._args.jdbc or self._args.ext == "jdbc":
            jdbc_config = self._config['benchmark']['jdbc-path']
        if self._args.ext == "remote":
            server_path = self._config['benchmark']['server-path']
        sh = SparkHelper(verbose=self._args.verbose, jdbc=jdbc_config,
                         server_path=server_path,
                         qflock_ds=self._args.qflock_ds,
                         output_path=self._args.output_path,
                         results_path=self._args.results_path)
        if self._args.jdbc:
            sh.load_extension()
        sh.init_extensions(self._args.ext)
        # This trace is important
        # the calling script will look for this before starting tracing.
        # Any traces before this point will *not* be seen at the default log level of OFF
        # if not self._args.no_catalog:
        #     self._create_default_catalog()
        if self._args.log_level:
            logging.info(f"Set log level to {self._args.log_level}")
            sh.set_log_level(self._args.log_level)
        if self._args.capture_log_level:
            logging.info(f"Set capture log level to {self._args.capture_log_level}")
            sh.set_log_level(self._args.capture_log_level)
        benchmark = self._get_benchmark(sh)
        BenchmarkApp._banner()
        if self._args.init_all:
            self._args.gen_data = True
            self._args.gen_parquet = True
            self._args.create_catalog = True
            self._args.compute_stats = True
            self._args.view_catalog = True
        if self._args.gen_data:
            logging.info("Starting to generate {} data".format(self._config['benchmark']['name']))
            benchmark.generate()
            logging.info("Generate {} data Complete.".format(self._config['benchmark']['name']))
        if self._args.gen_parquet:
            logging.info("Starting to generate {} parquet".format(self._config['benchmark']['name']))
            benchmark.write_parquet(self._config['benchmark']['raw-data-path'],
                                    self._config['benchmark']['parquet-path'])
            logging.info("Generate {} parquet Complete.".format(self._config['benchmark']['name']))
        if self._args.create_catalog:
            logging.info("Starting to create {} catalog".format(self._config['benchmark']['name']))
            benchmark.create_catalog()
            logging.info("Create {} catalog Complete".format(self._config['benchmark']['name']))
        if self._args.compute_stats:
            logging.info("Starting to compute {} stats".format(self._config['benchmark']['name']))
            sh.set_db(self._config['benchmark']['db-name'])
            benchmark.compute_stats()
            logging.info("Compute {} stats Complete".format(self._config['benchmark']['name']))
        if self._args.view_catalog:
            self.trace("View {} catalog Starting".format(self._config['benchmark']['name']))
            sh.get_catalog_info()
            self.trace("View {} catalog Complete".format(self._config['benchmark']['name']))
        if self._args.view_columns:
            sh.get_catalog_columns(self._args.view_columns)
        if self._args.delete_catalog:
            benchmark.delete_catalog()
        if self._args.no_catalog or self._args.jdbc or self._args.qflock_ds:
            if self._args.jdbc:
                logging.info(f"set database {self._config['benchmark']['db-name']}")
                sh.set_db(self._config['benchmark']['db-name'])
            benchmark.create_tables_view()
        if self._args.query_text or self._args.query_file or self._args.query_range:
            for i in range(0, self._args.loops):
                if self._args.query_text:
                    benchmark.query_text(self._args.query_text, self._args.explain,
                                         self._start_time)
                elif self._args.query_file:
                    benchmark.query_file(self._args.query_file, self._args.explain,
                                         self._start_time)
                elif self._args.query_range:
                    qc = self._get_query_config()
                    benchmark.query_range(qc, self._args.explain)
                if self._args.explain:
                    logging.info("see logs/explain.txt for output of explain")
                benchmark.cleanup()


if __name__ == "__main__":
    # from benchmark.benchmark import Benchmark
    # exception_list = ["2","43","59","13","48","24a","24b","75"]
    # q_list = Benchmark.get_query_list("*", "queries/tpcds", "sql", exception_list)
    # print(q_list)
    bench = BenchmarkApp()
    bench.run()

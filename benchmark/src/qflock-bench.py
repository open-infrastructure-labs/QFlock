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
import sys
import os
import time
import subprocess
import argparse
from argparse import RawTextHelpFormatter
import yaml

from benchmark.benchmark_factory import BenchmarkFactory
from framework_tools.spark_launcher import SparkLauncher
import bench
from benchmark.benchmark import Benchmark
from benchmark.config import Config
from framework_tools.spark_helper import SparkHelper


class QflockBench:
    """
    This represents a QFlock benchmark test, which launches the
    bench.py script with spark-submit.
    Most configuration data is found in the configuration .yaml
    """
    log_dir = "logs"
    def __init__(self):
        self._args = None
        self._remaining_args = None
        self._extra_args = ""
        self._query_list = []
        self._workers_list = []
        self._test_results = []
        self._startTime = time.time()
        self._test_failures = 0
        self._config = None
        self._spark_launcher = None
        self._wait_for_string = None
        self._exit_code = 0
        if not os.path.exists(QflockBench.log_dir):
            print(f"Creating directory: {QflockBench.log_dir}")
            os.mkdir(QflockBench.log_dir)

    def _parse_test_list(self):
        if self._args.queries:
            self._query_list = Benchmark.get_query_list(self._args.queries,
                                                        self._config['benchmark']['query-path'],
                                                        self._config['benchmark']['query-extension'],
                                                        self._config['benchmark']['query-exceptions'].split(","))

    def _parse_workers_list(self):
        increment = self._args.workers.split("+")
        if len(increment) > 1:
            inc = int(increment[1])
        else:
            inc = 1
        # print("worker inc : {0}".format(inc))
        test_items = increment[0].split(",")

        for i in test_items:
            if "-" in i:
                r = i.split("-")
                if len(r) == 2:
                    for t in range(int(r[0]), int(r[1]) + 1, inc):
                        self._workers_list.append(t)
            else:
                self._workers_list.append(int(i))
        # print("WorkerList {0}".format(self._workers_list))

    help_examples = """
      Examples:
        {0} --init_all
        {0} --generate --gen_parquet
        {0} --create_catalog --view_catalog
        {0} --compute_stats
        {0} --view_catalog
        {0} --view_catalog --verbose
        {0} --view_columns "*"
        {0} --view_columns "*" --verbose
        {0} --view_columns "web_site.*""
        {0} --view_columns "web_site.*"" --verbose
        {0} --view_columns "*.quantity"
        {0} --view_columns "*.name"
        # Run a query from test 25
        {0} --queries 25
        # Run a query from all tests
        {0} --queries *
        # Run a query from selected tests
        {0} --queries 1,5-8,22-30,42,55-70
        # Run a query from text input
        {0} --query_text "select count(1) from web_site"
        {0} --query_text "select cc_street_name,cc_city,cc_state from call_center" --verbose
        # Run a query from text in a file
        {0} --query_file 2.sql
        # Explain a query
        {0} --explain --query_range 3 --log_level INFO
        {0} --explain --query_range "*"
                                             """

    # python3 ./bench.py -f config_tpcds.yaml --query_text "DESC EXTENDED tpcds.web_site web_site_sk" -v
    def get_parser(self, parents=[]):
        parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter,
                                         description="App for running Qflock Benchmarks on Spark.\n",
                                         epilog=QflockBench.help_examples.format(sys.argv[0]),
                                         parents=parents)
        parser.add_argument("--debug", "-dbg", action="store_true",
                            help="enable debugger")
        parser.add_argument("--verbose", "-v", action="store_true",
                            help="Increase verbosity of output.")
        parser.add_argument("--terse", "-t", action="store_true",
                            help="Limited output.")
        parser.add_argument("--extensions", "-ext", default=None,
                            help="enable extensions (explain, jdbc, ds)")
        parser.add_argument("--log_level", "-ll", default="OFF",
                            help="log level set to input arg.\n"
                                 "Valid values are OFF, ERROR, WARN, INFO, DEBUG, TRACE")
        parser.add_argument("--file", "-f", default="config.yaml",
                            help="config file to use, defaults to spark_bench.yaml")
        parser.add_argument("--queries", "-q", default=None,
                            help="queries to run by spark_bench.py\n"
                                 "ex. -q 1,2,3,5-9,16-19,21,*")
        parser.add_argument("--query_range", "-qr", default=None,
                            help="queries to run directly by bench.py\n"
                                 "ex. -q 1,2,3,5-9,16-19,21,*")
        parser.add_argument("--view_columns",
                            help="use <table>.<column>\n"
                                 "ex. web_site.web_city or web_site.* or *.web_city")
        parser.add_argument("--workers", "-w", default="1",
                            help="worker threads\n"
                                 "ex. -w 1,2,3,5-9,16-19,21")
        parser.add_argument("--query_text", default=None,
                            help="Query to try")
        parser.add_argument("--results", "-r", default="results.csv",
                            help="results file\n"
                                 "ex. -r results.csv")
        parser.add_argument("--iterations", default="1",
                            help="number of iterations of queries")
        parser.add_argument("--name", "-n", default="",
                            help="name for test")
        parser.add_argument("--catalog_name",  default="default",
                            help="catalog to use (maps to port)")
        parser.add_argument("--dump_tables", action="store_true",
                            help="cause every row from every table to be generated as output.")
        parser.add_argument("--output_path", default=None,
                            help="root folder to output results.")
        parser.add_argument("--results_path", default=None,
                            help="directory for perf results.")
        parser.add_argument("--results_file", default=None,
                            help="file for perf results.")
        parser.add_argument("--restart_jdbc", action="store_true",
                            help="Restart the jdbc server.")
        parser.add_argument("--restart_remote", action="store_true",
                            help="Restart the remote server.")
        return parser

    def _terse_command(self):
        # If we are logging then we should not invoke terse mode.
        if self._args.log_level != "OFF":
            return False
        elif self._args.queries is not None or self._args.query_range is not None:
            # If we are using certain commands, allow terse mode.
            return True
        else:
            return False
        
    def _parse_args(self):
        b = bench.BenchmarkApp()
        parent_parser = b.get_parser(parent_parser=True)

        # Combined parser allows us to serve up one combined help page.
        combined_parser = self.get_parser([parent_parser])
        combined_parser.parse_known_args()

        # If we make it here, we know that help is not an option,
        # proceed with parser for just this script.
        parser = self.get_parser()
        self._args, self._remaining_args = parser.parse_known_args()
        self._parse_workers_list()
        self._wait_for_string = None
        if not self._args.verbose and self._terse_command():
            # IF we are not verbose Then we are terse by default for certain commands.
            self._args.terse = True
        if not self._args.terse:
            if self._args.log_level == "OFF":
                self._wait_for_string = "bench.py starting"
            else:
                self._wait_for_string = None
        if "--capture_log_level" in self._remaining_args:
            self._wait_for_string = None
        return True

    def process_cmd_status(self, output):
        for line in output:
            if ("Cmd Failed" in line) or ("FAILED" in line):
                return False
        return True

    def show_results(self):
        if os.path.exists(self._args.results):
            mode = "a"
        else:
            mode = "w"
        with open(self._args.results, mode) as fd:
            fd.write("Test: {0}\n".format(self._remaining_args))
            for r in self._test_results:
                print(r.rstrip())
                fd.write(r.rstrip() + "\n")

    def display_elapsed(self):
        end = time.time()
        hours, rem = divmod(end - self._startTime, 3600)
        minutes, seconds = divmod(rem, 60)
        print("elapsed time: {:2}:{:02}:{:02}".format(int(hours), int(minutes), int(seconds)))

    def restart_jdbc(self):
        ssh_cmd = "ssh qflock-jdbc-dc2 /scripts/restart_jdbc.sh"
        result = subprocess.run(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                shell=True)
        if result.returncode != 0:
            print("failed to restart jdbc")
        print("restarted jdbc.  Waiting 60 seconds for server to come up.")
        # TODO We should really poll the server to check it is up.
        time.sleep(60)

    def restart_remote(self):
        ssh_cmd = "ssh qflock-spark-dc2 /scripts/restart_server.sh"
        result = subprocess.run(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                shell=True)
        if result.returncode != 0:
            print("failed to restart remote server")
        print("restarted remote server.  Waiting 60 seconds for server to come up.")
        # TODO We should really poll the server to check it is up.
        time.sleep(60)

    def run_query(self):
        # timestr = time.strftime("%Y%m%d-%H%M%S")
        failure_count = 0
        success_count = 0
        for w in self._workers_list:
            idx = 0
            for q in self._query_list:
                if idx != 0 and self._args.restart_jdbc and (idx % 10) == 0:
                    self.restart_jdbc()
                if idx != 0 and self._args.restart_remote and (idx % 10) == 0:
                    self.restart_remote()
                cmd = f'./bench.py -f {self._args.file} -ll {self._args.log_level} ' + \
                      f'--query_file {q} {" ".join(self._remaining_args)} ' + \
                      f'--test_num {idx} '
                if self._args.results_path:
                    cmd += f'--results_path {self._args.results_path} '
                if self._args.results_file:
                    cmd += f'--results_file {self._args.results_file} '
                if self._args.extensions == "explain":
                    # Auto enable explain on this query if we are using explain extension.
                    cmd += '--explain --ext explain'
                if self._args.extensions == "jdbc":
                    cmd += '--ext jdbc '
                if self._args.extensions == "remote":
                    cmd += '--ext remote '
                if self._args.extensions == "ds":
                    # Auto enable qflock_ds if we are using qflock_ds extension.
                    cmd += '--qflock_ds '
                if self._args.output_path:
                    cmd += f'--output_path {self._args.output_path} '
                rc, output = self._spark_launcher.spark_submit(cmd, workers=w,
                                                               enable_stdout=self._args.log_level != "OFF",
                                                               wait_text=self._wait_for_string)
                output_status = self.process_cmd_status(output)
                if rc != 0:
                    self._exit_code = rc
                if not output_status or rc != 0:
                    failure_count += 1
                else:
                    success_count += 1
                idx += 1
        # self.show_results()
        #self.display_elapsed()

        print("")
        print(f"SUCCESS: {success_count} FAILURE: {failure_count}")

    def run_cmd(self):
        cmd = f'./bench.py -f {self._args.file} -ll {self._args.log_level} '
        if self._args.output_path:
            cmd += f'--output_path {self._args.output_path} '
        if self._args.results_path:
            cmd += f'--results_path {self._args.results_path} '
        if self._args.results_file:
            cmd += f'--results_file {self._args.results_file} '
        if self._args.query_range:
            cmd += f'--query_range "{self._args.query_range}" '
        if self._args.query_text:
            cmd += f'--query_text "{self._args.query_text}" '
        if self._args.view_columns:
            cmd += f'--view_columns "{self._args.view_columns}" '
        if self._args.extensions == "explain":
            # Auto enable explain on this query if we are using explain extension.
            cmd += '--explain --ext explain '
        if self._args.extensions == "jdbc":
            cmd += '--ext jdbc '
        if self._args.extensions == "remote":
            cmd += '--ext remote '
        if self._args.extensions == "ds":
            # Auto enable qflock_ds if we are using qflock_ds extension.
            cmd += '--qflock_ds '
        cmd += " ".join(self._remaining_args)
        cmd += self._extra_args

        for w in self._workers_list:
            rc, output = self._spark_launcher.spark_submit(cmd, workers=w,
                                                           enable_stdout=self._args.log_level != "OFF",
                                                           wait_text=self._wait_for_string)
            if rc != 0:
                self._exit_code = rc
            #print(output)

    def dump_tables(self):
        output_path = self._args.output_path
        self._args.output_path = None
        new_bench = BenchmarkFactory.get_benchmark(self._config, None)
        for t in new_bench.tables.get_tables():
            self._extra_args = f" --output_path {output_path}/table_{t}"
            self._args.query_text = f"select * from {t}"
            self.run_cmd()

    def run(self):
        if not self._parse_args():
            return
        self._config = Config(self._args.file)
        self._parse_test_list()
        self._spark_launcher = SparkLauncher(self._config['spark'], self._args)

        loops = int(self._args.iterations)
        for loop in range(0, loops):
            if self._query_list:
                self.run_query()
            elif self._args.dump_tables:
                self.dump_tables()
            else:
                self.run_cmd()
        exit(self._exit_code)


if __name__ == "__main__":
    app = QflockBench()
    app.run()

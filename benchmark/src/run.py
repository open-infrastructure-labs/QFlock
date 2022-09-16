#!/usr/bin/env python3
import time
import datetime
import subprocess
from datetime import datetime
from benchmark.benchmark import Benchmark
from benchmark.config import Config



def list_batches(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

class RunBenchmark:
    def __init__(self, tests="*",
                 results_path=None,
                 results_file="results.csv",
                 extra_args=""):
        self._tests = tests
        self._results_file = results_file
        self._extra_args = extra_args
        if results_path is None:
            now = datetime.now()
            self._results_path = now.strftime("results/%m-%d-%Y")
        self._config = Config("config.yaml")

    def run_test(self, tests=None):
        cmd = f'./docker-bench.py --queries "{tests}" --results_path={self._results_path} ' + \
              f"--results_file={self._results_file} {self._extra_args}"
        print(cmd)
        status = subprocess.call(cmd, shell=True)
        if status != 0:
            print(f"\n*** Exit code was {status}")
            exit(status)

    def get_test_list(self):
        query_list = Benchmark.get_query_list(self._tests,
                                              self._config['benchmark']['query-path'],
                                              self._config['benchmark']['query-extension'],
                                              self._config['benchmark']['query-exceptions'].split(","))
        query_list = [s.replace("queries/tpcds/", "").replace(".sql", "") for s in query_list]
        return query_list

    def run_tests(self, batch_size=5):
        query_list = self.get_test_list()
        tests = list(list_batches(query_list, batch_size))
        started = False
        for batch in tests:
            if started:
                self.restart()
            test_string = ",".join(batch)
            print(f"batch is: {test_string}")
            self.run_test(test_string)
            started = True

    def restart(self):
        print("restart")
        status = subprocess.call("scripts/stop.sh", shell=True)
        if status != 0:
            print(f"\n*** Exit code was {status}")
            exit(status)
        time.sleep(5)
        status = subprocess.call("scripts/start.sh", shell=True)
        if status != 0:
            print(f"\n*** Exit code was {status}")
            exit(status)
        time.sleep(10)


if __name__ == "__main__":
    # jdbc = RunBenchmark(tests="*", results_file="jdbc_results.csv",
    #                     extra_args="--ext jdbc --restart_jdbc")
    # jdbc.run_test()
    #
    # spark = RunBenchmark(tests="*", results_file="spark_results.csv")
    # spark.run_test()
    test_list = "*"
    # test_list = "17,17,21,25,29,33,37,39a,39b,47,49,50,53,56,58,60,61,63,64,70,76,82,83,89,93,98"
    compact = RunBenchmark(tests=test_list, results_file="remote_results.csv",
                           extra_args="--ext remote")
    compact.run_tests()

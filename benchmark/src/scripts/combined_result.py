#!/usr/bin/env python3
import os
import sys
import csv


class TestResult:
    def __init__(self, d: dict):
        self.status = d['status']
        self.name = d['query'].replace(".sql", "")
        self.seconds = d['seconds']
        self.rows = d['rows']


class GenerateCombinedResult:
    def __init__(self, spark_file, qflock_file, data_dir):
        self._spark_file = spark_file
        self._qflock_file = qflock_file
        self._data_dir = data_dir
        self._spark_results = {}
        self._qflock_results = {}

    def parse_results(self, file):
        results = {}
        with open(file, 'r') as fd:
            for line in fd.readlines():
                if "query" not in line and 'PASSED' in line:
                    items = line.split(",")
                    result = {'status': items[0],
                              'rows': items[2],
                              'seconds': items[3]}
                    results[items[1]] = result
        return results

    def load_tables(self, file_name: str):
        tables = dict()
        with open(file_name, newline='') as csv_file:
            reader = csv.DictReader(csv_file, delimiter=',')
            for row in reader:
                t = TestResult(row)
                tables[t.name] = t
        return tables

    def gen_combined_result(self):
        with open(os.path.join(self._data_dir, "combined_results.csv"), "w") as fd:
            fd.write("query,Jdbc Seconds,Spark Seconds\n")
            for r in self._qflock_results:
                if self._qflock_results[r].status == 'PASSED' and \
                   r in self._spark_results:
                    name = r.replace(".sql", "")
                    fd.write(f"{name},{self._qflock_results[r].seconds},"
                             f"{self._spark_results[r].seconds}\n")

    def run(self):
        self._spark_results = self.load_tables(self._spark_file)
        self._qflock_results = self.load_tables(self._qflock_file)

        self.gen_combined_result()


if __name__ == "__main__":
    if len(sys.argv) > 2:
        cr = GenerateCombinedResult(sys.argv[1], sys.argv[2])
    else:
        cr = GenerateCombinedResult("data/spark_results.csv",
                                    "data/jdbc_results.csv",
                                    "data")
    cr.run()

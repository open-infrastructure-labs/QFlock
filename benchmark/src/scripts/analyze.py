#!/usr/bin/env python3
import sys
import os
import csv
import re
import argparse
from argparse import RawTextHelpFormatter
from parse_qflock_log import ParseQflockLog


class Table:

    def __init__(self, d: dict):
        self.name = d['table name']
        self.path = d['path']
        self.location = d['data center']
        self.bytes = d['bytes']
        self.rows = d['metastore rows']
        self.row_groups = d['metastore row groups']
        self.pq_rows = d['parquet rows']
        self.pq_row_groups = d['parquet row groups']


class Query:
    def __init__(self, d: dict):
        self.name = d['query']
        self.dc1_table_names = d['dc1 tables'].split()
        self.dc2_table_names = d['dc2 tables'].split()
        self.dc1_tables = dict()
        self.dc2_tables = dict()

class CombinedResult:
    def __init__(self, d: dict):
        self.name = d['query']
        self.jdbc = d['Jdbc Seconds']
        self.spark = d['Spark Seconds']
        if self.jdbc == '' or self.spark == '':
            self.jdbc = 0.0
            self.spark = 0.0
            self.gain = 0.0
        else:
            self.jdbc = float(d['Jdbc Seconds'])
            self.spark = float(d['Spark Seconds'])
            self.gain = (self.spark - self.jdbc) / self.spark

        self.query = dict()

class Result:
    def __init__(self, d: dict):
        self.name = d['query']
        self.status = d['status']
        self.rows = int(d['rows'])
        self.seconds = float(d['seconds'])
        self.jdbc_bytes = int(d['qflock-storage-dc1:tx_bytes:eth0']) + int(d['qflock-jdbc-dc2:tx_bytes:eth1'])
        self.spark_bytes = int(d['qflock-storage-dc1:tx_bytes:eth0']) + int(d['qflock-storage-dc2:tx_bytes:eth1'])


class AnalyzeData:
    functions = ["best_fit", "remote_table_filter", "top_10g", "compare"]

    def __init__(self):
        self._args = None
        self._data_dir = None
        self._tables = None
        self._queries = None
        self._combined_results = None
        self._jdbc_results = None
        self._spark_results = None
        self._qflock_log = None
        self._qflock_log_by_test = None

    def load_data(self):
        self.load_tables(os.path.join(self._data_dir, 'tables.csv'))
        self.load_queries(os.path.join(self._data_dir, 'queries.csv'))
        self._combined_results = self.load_combined_results(os.path.join(self._data_dir, 'combined_results.csv'))
        self._jdbc_results = self.load_results(os.path.join(self._data_dir, 'jdbc_results.csv'))
        self._spark_results = self.load_results(os.path.join(self._data_dir, 'spark_results.csv'))
        qflock_log = ParseQflockLog(os.path.join(self._data_dir, "qflock_log.txt"))
        self._qflock_log = qflock_log.log
        self._qflock_log_by_test = qflock_log.log_by_test

    def curate_data(self):
        for k, q in self._queries.items():
            for t in q.dc1_table_names:
                q.dc1_tables[t] = self._tables[t]

            for t in q.dc2_table_names:
                q.dc2_tables[t] = self._tables[t]

        for k, r in self._combined_results.items():
            if r.name in self._queries.keys():
                r.query = self._queries[r.name]

    def load_tables(self, file_name: str):
        tables = dict()
        with open(file_name, newline='') as csv_file:
            reader = csv.DictReader(csv_file, delimiter=',')
            for row in reader:
                t = Table(row)
                tables[t.name] = t
        self._tables = tables

    def load_queries(self, file_name: str):
        queries = dict()
        with open(file_name, newline='') as csv_file:
            reader = csv.DictReader(csv_file, delimiter=',')
            for row in reader:
                q = Query(row)
                queries[q.name] = q
        self._queries = queries

    def load_combined_results(self, file_name: str):
        results = dict()
        with open(file_name, newline='') as csv_file:
            reader = csv.DictReader(csv_file, delimiter=',')
            for row in reader:
                r = CombinedResult(row)
                results[r.name] = r
        return results

    def load_results(self, file_name: str):
        results = dict()
        with open(file_name, newline='') as csv_file:
            reader = csv.DictReader(csv_file, delimiter=',')
            for row in reader:
                r = Result(row)
                results[r.name] = r
        return results

    def best_fit(self):

        spark_time = [r.spark for r in self._combined_results.values()]
        spark_time.sort(reverse=True)
        # print(spark_time)
        baseline_threshold = spark_time[len(spark_time)//2]  # 50 % of results

        gain = [r.gain for r in self._combined_results.values()]
        gain.sort(reverse=True)
        gain_threshold = gain[len(gain) // 10]  # 10 % of results

        for r in self._combined_results.values():
            print(f"{r.name},{r.spark},{r.jdbc},{baseline_threshold},{r.gain},{gain_threshold}," +
                  f"{r.query.dc1_table_names},{r.query.dc2_table_names}")
            if r.spark < baseline_threshold:
                continue
            if r.gain < gain_threshold:
                continue

            if len(r.query.dc1_table_names) < 2 or len(r.query.dc1_table_names) > 3:
                continue

            if len(r.query.dc2_table_names) < 2 or len(r.query.dc2_table_names) > 3:
                continue

            print(r.name, r.spark, r.jdbc, f'{r.gain*100:.0f}',
                  len(r.query.dc1_table_names),
                  len(r.query.dc2_table_names))
            q = r.query
            msg = "DC 1 Tables: "
            for t in q.dc1_tables.values():
                msg += f'{t.name}, {t.rows}; '

            print(msg)

            msg = "DC 2 Tables: "
            for t in q.dc2_tables.values():
                msg += f'{t.name}, {t.rows}; '

            print(msg)

    def remote_table_filter(self):
        found_count = 0
        gain_count = 0
        print("query,filters,remote table,gain")
        for r in self._combined_results.values():
            filter_found = False
            remote_table_found = False
            if r.name in self._qflock_log_by_test:
                query = r.name
                query_log = self._qflock_log_by_test[query]
                filters = []
                for query_key, query in query_log['queries'].items():
                    # print(query['query'])
                    if self._tables[query['table']].location == 'dc2':
                        remote_table_found = True
                        query_sql = query['query']
                        where_items = query_sql.split("WHERE")
                        if len(where_items) > 1:
                            where_clause = where_items[1].lstrip(" ").rstrip(" ")
                            filtered_where_clause = re.sub("(AND)*\\s*\\w+ IS NOT NULL\\s*(AND)*",
                                                           "", where_clause)
                            filters.append(where_clause)
                            if filtered_where_clause != "":
                                filter_found = True
            if not filter_found or not remote_table_found:
                print(f"{r.name},{filter_found},{remote_table_found},{r.gain},{r.spark},{r.jdbc}")
                found_count += 1
                if r.gain > 0.2:
                    gain_count += 1
        print(f"{found_count} queries found ({gain_count}) with gains > 5%")

    def top_10g(self):
        results_10g = self.load_combined_results(os.path.join(self._data_dir, 'combined_results_10g.csv'))

        results_10g_sorted = \
            [(k, results_10g[k].gain) for k in sorted(results_10g, key=lambda x: results_10g[x].gain, reverse=True)]

        index = 0
        print("test index,test number,10g gain,100g gain,gain difference")
        for k in results_10g_sorted:
            if index > 54:
                 break
            diff = self._combined_results[k[0]].gain - k[1]
            print(index, k[0], k[1], self._combined_results[k[0]].gain, diff, sep=",")
            # if (diff < 0.0):
            #      print(index, k[0], k[1], self._combined_results[k[0]].gain, diff, sep=",")
            # if (diff > 0.05):
            #      print(index, k[0], k[1], self._combined_results[k[0]].gain, diff)
            index += 1

    def compare(self):
        print("query,jdbc seconds,spark seconds,gain time,jdbc bytes,spark bytes,gain bytes")
        for query, jdbc_result in self._jdbc_results.items():
            spark_result = self._spark_results[query]

            gain_time = (spark_result.seconds - jdbc_result.seconds) / spark_result.seconds
            gain_bytes = (spark_result.spark_bytes - jdbc_result.jdbc_bytes) / spark_result.spark_bytes
            self._jdbc_results[query].gain_time = gain_time
            self._jdbc_results[query].gain_bytes = gain_bytes
            print(query, jdbc_result.seconds, spark_result.seconds, round(gain_time * 100, 4),
                  jdbc_result.jdbc_bytes, spark_result.spark_bytes, round(gain_bytes * 100, 4),
                  sep=",")

        results_sorted = \
            [k for k in sorted(self._jdbc_results, key=lambda x: self._jdbc_results[x].gain_time, reverse=True)]

        for query in results_sorted:
            spark_result = self._spark_results[query]
            jdbc_result = self._jdbc_results[query]
            print(query, jdbc_result.seconds, spark_result.seconds, round(jdbc_result.gain_time * 100, 4),
                  jdbc_result.jdbc_bytes, spark_result.spark_bytes, round(jdbc_result.gain_bytes * 100, 4),
                  sep=",")


    def parse_args(self):
        parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter,
                                         description="App to analyze benchmark data.\n")
        parser.add_argument("--dir", "-D", default=None, required=True,
                            help="folder for data files")
        parser.add_argument("--func", default=None, required=True,
                            help="analysis function. \"--func list\" for list of functions.")
        self._args = parser.parse_args()

        if not os.path.exists(self._args.dir):
            print(f"--dir requires a valid folder. {self._args.dir} is not valid")
            exit(1)
        else:
            self._data_dir = self._args.dir

    def run(self):
        self.parse_args()
        if self._args.func == "list":
            print("available functions:")
            for f in AnalyzeData.functions:
                print("  " + f)
        else:
            self.load_data()
            self.curate_data()
            if self._args.func == "best_fit":
                self.best_fit()
            elif self._args.func == "remote_table_filter":
                self.remote_table_filter()
            elif self._args.func == "top_10g":
                self.top_10g()
            elif self._args.func == "compare":
                self.compare()
            else:
                print(f"Unknown function {self._args.func}")


if __name__ == '__main__':
    a = AnalyzeData()
    a.run()

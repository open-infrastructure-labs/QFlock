#!/usr/bin/env python3
import os
import csv
import re
import argparse
from argparse import RawTextHelpFormatter
from combined_result import GenerateCombinedResult
from parse_qflock_join_log import ParseQflockJoinLog
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
        self.jdbc_bytes = int(d['qflock-storage-dc1:tx_bytes:eth0']) + int(d['qflock-spark-dc2:tx_bytes:eth1'])
        self.spark_bytes = int(d['qflock-storage-dc1:tx_bytes:eth0']) + int(d['qflock-storage-dc2:tx_bytes:eth1'])
        self.jdbc_remote_bytes =  int(d['qflock-spark-dc2:tx_bytes:eth1'])
        self.spark_remote_bytes = int(d['qflock-storage-dc2:tx_bytes:eth1'])
        self.jdbc_local_bytes = int(d['qflock-storage-dc1:tx_bytes:eth0'])
        self.spark_local_bytes = int(d['qflock-storage-dc1:tx_bytes:eth0'])


class AnalyzeData:
    functions = ["best_fit", "remote_table_filter", "top_10g", "compare",
                 "join_stats", "jdbc_compare", "join_runtime_stats"]

    def __init__(self):
        self._data_dir = ""
        self._tables = None
        self._queries = None
        self._combined_results = None
        self._jdbc_results = None
        self._jdbc_results2 = None
        self._baseline_results = None
        self._qflock_log = None
        self._qflock_log_by_test = None
        self._args = self.parse_args()
        self._data_dir = self._args.dir
        self._combined_path = os.path.join(self._data_dir, 'combined_results.csv')
        self._jdbc_path = os.path.join(self._data_dir, self._args.results_file)
        self._jdbc_path2 = os.path.join(self._data_dir, self._args.results_file2)
        self._baseline_path = os.path.join(self._data_dir, self._args.baseline_file)
        self._qflock_path = os.path.join(self._data_dir, "qflock_log.txt")
        self.generate_results()

    def load_data(self):
        self.load_tables(os.path.join(self._data_dir, 'tables.csv'))
        self.load_queries(os.path.join(self._data_dir, 'queries.csv'))
        if os.path.exists(self._combined_path):
            self._combined_results = self.load_combined_results(self._combined_path)
        if os.path.exists(self._jdbc_path):
            self._jdbc_results = self.load_results(self._jdbc_path)
        if os.path.exists(self._jdbc_path2):
            self._jdbc_results2 = self.load_results(self._jdbc_path2)
        if os.path.exists(self._baseline_path):
            self._baseline_results = self.load_results(self._baseline_path)
        if os.path.exists(self._qflock_path):
            qflock_log = ParseQflockLog(self._qflock_path)
            self._qflock_log = qflock_log.log
            self._qflock_log_by_test = qflock_log.log_by_test

    def curate_data(self):
        for k, q in self._queries.items():
            for t in q.dc1_table_names:
                q.dc1_tables[t] = self._tables[t]

            for t in q.dc2_table_names:
                q.dc2_tables[t] = self._tables[t]
        if self._combined_results:
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
                if row['status'] == 'PASSED':
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
        print("query,qflock seconds,spark seconds,gain time,qflock total bytes,spark total bytes,gain bytes," +
              "qflock remote bytes,spark remote bytes,gain bytes," +
              "qflock remote bytes % of total,spark remote bytes % of total," +
              "qflock local bytes,spark local bytes,gain bytes,")
        for query, jdbc_result in self._jdbc_results.items():
            spark_result = self._baseline_results[query]

            gain_time = (spark_result.seconds - jdbc_result.seconds) / spark_result.seconds
            gain_bytes = (spark_result.spark_bytes - jdbc_result.jdbc_bytes) / spark_result.spark_bytes
            gain_local_bytes = \
                (spark_result.spark_local_bytes - jdbc_result.jdbc_local_bytes) / spark_result.spark_local_bytes
            gain_remote_bytes = \
                (spark_result.spark_remote_bytes - jdbc_result.jdbc_remote_bytes) / spark_result.spark_remote_bytes
            self._jdbc_results[query].gain_time = gain_time
            self._jdbc_results[query].gain_bytes = gain_bytes
            qflock_remote_bytes_pct_of_total = jdbc_result.jdbc_remote_bytes / jdbc_result.jdbc_bytes
            spark_remote_bytes_pct_of_total = spark_result.spark_remote_bytes / spark_result.spark_bytes
            print(query.replace(".sql", ""),
                  jdbc_result.seconds, spark_result.seconds, round(gain_time * 100, 4),
                  jdbc_result.jdbc_bytes, spark_result.spark_bytes, round(gain_bytes * 100, 4),
                  jdbc_result.jdbc_remote_bytes, spark_result.spark_remote_bytes, round(gain_remote_bytes * 100, 4),
                  round(qflock_remote_bytes_pct_of_total * 100, 4), round(spark_remote_bytes_pct_of_total * 100, 4),
                  jdbc_result.jdbc_local_bytes, spark_result.spark_local_bytes, round(gain_local_bytes * 100, 4),
                  sep=",")

        results_sorted = \
            [k for k in sorted(self._jdbc_results, key=lambda x: self._jdbc_results[x].gain_bytes, reverse=False)]

        # for query in results_sorted:
        #     spark_result = self._baseline_results[query]
        #     jdbc_result = self._jdbc_results[query]
        #     print(query, jdbc_result.seconds, spark_result.seconds, round(jdbc_result.gain_time * 100, 4),
        #           jdbc_result.jdbc_bytes, spark_result.spark_bytes, round(jdbc_result.gain_bytes * 100, 4),
        #           sep=",")

    def join_stats(self):
        join_log = ParseQflockJoinLog(os.path.join(self._data_dir, "qflock_log.txt"))

        all_remote = []
        results = {}
        for name, info in join_log.log.items():
            results[name] = {}
            for tables, join_info in info['joins'].items():
                results[name][tables] = []
                result = {'local_count': 0,
                          'remote_count': 0,
                          'small_local_count': 0,
                          'small_remote_count': 0}
                # if "Outer" in join_info['join_type']:
                #     print(f"{name} found join_type {join_info['join_type']}")
                #     continue
                # print(join_info)
                for t in join_info['tables']:
                    if self._tables[t].location == 'dc1':
                        result['local_count'] += 1
                        if int(self._tables[t].bytes) < (1024 * 1024 * 50):
                            result['small_local_count'] += 1
                            # if join_info['join_type'] != "Inner":
                            #     print(f"{name} small_local join_type:{join_info['join_type']}")
                    else:
                        # if join_info['join_type'] != "Inner":
                        #     print(f"{name} remote join_type:{join_info['join_type']}")
                        result['remote_count'] += 1
                        if int(self._tables[t].bytes) < (1024 * 1024 * 10):
                            result['small_remote_count'] += 1
                # if result['remote_count'] == 2:
                if result['remote_count'] == 2:
                    print(f"{name} {join_info['tables']} {join_info['join_type']}")
                    all_remote.append(name)
                results[name][tables].append(result)
        stats = {'small_local_count': 0,
                 'all_remote': 0}
        for name, results_dict in results.items():
            query_stats = {'small_local_count': 0,
                           'all_remote': 0}
            for t, join in results_dict.items():
                for j in join:
                    if j['small_local_count'] == 1 and j['remote_count'] == 1:
                        stats['small_local_count'] += 1
                        query_stats['small_local_count'] += 1
                    if j['remote_count'] == 2:
                        stats['all_remote'] += 1
                        query_stats['all_remote'] += 1

            print(f"query:{name} all_remote:{query_stats['all_remote']} " +
                  f"small_local:{query_stats['small_local_count']}")
        print("totals")
        print(80*"-")
        print(f"all_remote:{stats['all_remote']} " +
              f"small_local:{stats['small_local_count']}")
        print(f"all_remote: {','.join(sorted(set(all_remote)))}")

    def jdbc_compare(self):
        print("query,qflock seconds,jdbc base seconds,spark seconds," +
              "jdbc spark gain time,jdbc base spark gain time,jdbc gain time," +
              "jdbc bytes,jdbc base bytes,spark bytes," +
              "jdbc spark gain bytes,jdbc base spark gain bytes,jdbc gain bytes")
        for query, jdbc_result in self._jdbc_results.items():
            spark_result = self._baseline_results[query]
            jdbc_base_result = self._jdbc_results2[query]

            jdbc_gain_time = (jdbc_base_result.seconds - jdbc_result.seconds) / jdbc_base_result.seconds
            jdbc_gain_bytes = (spark_result.jdbc_bytes - jdbc_result.jdbc_bytes) / jdbc_base_result.jdbc_bytes

            jdbc_spark_gain_time = (spark_result.seconds - jdbc_result.seconds) / spark_result.seconds
            jdbc_spark_gain_bytes = (spark_result.spark_bytes - jdbc_result.jdbc_bytes) / spark_result.spark_bytes

            jdbc_base_spark_gain_time = (spark_result.seconds - jdbc_base_result.seconds) / spark_result.seconds
            jdbc_base_spark_gain_bytes = (spark_result.spark_bytes - jdbc_base_result.jdbc_bytes) \
                / spark_result.spark_bytes

            print(query.replace(".sql", ""),
                  jdbc_result.seconds, jdbc_base_result.seconds, spark_result.seconds,
                  round(jdbc_spark_gain_time, 4), round(jdbc_base_spark_gain_time, 4), round(jdbc_gain_time, 4),
                  jdbc_result.jdbc_bytes, jdbc_base_result.jdbc_bytes, spark_result.spark_bytes,
                  round(jdbc_gain_bytes, 4), round(jdbc_spark_gain_bytes, 4), round(jdbc_base_spark_gain_bytes, 4),
                  sep=",")

    def jdbc_compare_old(self):
        print("query,jdbc seconds,spark seconds,gain time,jdbc bytes,spark bytes,gain bytes")
        for query, jdbc_result in self._jdbc_results.items():
            spark_result = self._baseline_results[query]

            gain_time = (spark_result.seconds - jdbc_result.seconds) / spark_result.seconds
            gain_bytes = (spark_result.jdbc_bytes - jdbc_result.jdbc_bytes) / spark_result.jdbc_bytes
            self._jdbc_results[query].gain_time = gain_time
            self._jdbc_results[query].gain_bytes = gain_bytes
            print(query.replace(".sql", ""),
                  jdbc_result.seconds, spark_result.seconds, round(gain_time * 100, 4),
                  jdbc_result.jdbc_bytes, spark_result.jdbc_bytes, round(gain_bytes * 100, 4),
                  sep=",")

    def join_runtime_stats(self):
        print("test,left table,left rows,right table,right rows," +
              "result rows,result bytes,join_expression,filters,query")
        for (test, log) in self._qflock_log_by_test.items():
            # print(f"{test}:{log['app_id']}")
            for query_info in log['queries'].values():
                sql = query_info['query']
                # Remove the IS NOT NULL checks.
                # sql = re.sub("\w+ IS NOT NULL", "", sql)
                if "ON" not in sql:
                    continue
                items = sql.split("ON")
                expression = items[1].replace("WHERE", "").lstrip(" ").rstrip(" ")
                from_items = sql.split("FROM ")
                tables = [from_items[2].split(" ")[0]]
                tables.append(from_items[3].split(" ")[0])
                left = tables[0]
                right = tables[1]
                filter = "hasfilters" in query_info['rule_log']
                print(f'{test},{left},{self._tables[left].rows},' +
                      f'{right},{self._tables[right].rows},' +
                      f'{query_info["rows"]},{query_info["bytes"]},' +
                      f'"{expression}","{filter}","{sql}"')

    def parse_args(self):
        parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter,
                                         description="App to analyze benchmark data.\n")
        parser.add_argument("--dir", "-D", default=None, required=True,
                            help="folder for data files")
        parser.add_argument("--func", default=None, required=True,
                            help="analysis function. \"--func list\" for list of functions.")
        parser.add_argument("--baseline_file", default="spark_results.csv",
                            help="The .csv file with the baseline data.")
        parser.add_argument("--results_file", default="jdbc_results.csv",
                            help="The .csv file with the data to analyze.")
        parser.add_argument("--results_file2", default="baseline_results.csv",
                            help="The .csv file to compare against results_file.")
        args = parser.parse_args()
        if not os.path.exists(args.dir):
            print(f"--dir requires a valid folder. {args.dir} is not valid")
            exit(1)
        return args

    def generate_results(self):
        if not os.path.exists(os.path.join(self._args.dir, "combined_results.csv")):
            if os.path.exists(self._baseline_path) and os.path.exists(self._jdbc_path):
                cr = GenerateCombinedResult(self._baseline_path,
                                            self._jdbc_path,
                                            self._args.dir)
                cr.run()

    def run(self):
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
            elif self._args.func == "join_stats":
                self.join_stats()
            elif self._args.func == "jdbc_compare":
                self.jdbc_compare()
            elif self._args.func == "join_runtime_stats":
                self.join_runtime_stats()
            else:
                print(f"Unknown function {self._args.func}")


if __name__ == '__main__':
    a = AnalyzeData()
    a.run()

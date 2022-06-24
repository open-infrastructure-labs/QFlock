#!/usr/bin/env python3
import os
import sys
import re
import glob


class QueryInfo:

    def __init__(self, root_path, dest_path):
        self._root_path = root_path
        self._dest_path = dest_path
        self._queries = {}
        self._table_counts = {}
        self._table_info = self.get_table_metadata(os.path.join(self._dest_path, "tables.csv"))
        self._tables = [f['name'] for f in self._table_info]
        self._table_data_center = {f['name']: f['datacenter'] for f in self._table_info}
        for t in self._tables:
            self._table_counts[t] = 0

    def get_table_metadata(self, file):
        tables = []
        with open(file, 'r') as fd:
            for line in fd.readlines():
                items = line.split(",")
                table = {'name': items[0], 'datacenter': items[2],
                         'path': items[1]}
                tables.append(table)
        return tables

    def process_file(self, file):
        with open(file, 'r') as fd:
            dc1 = []
            dc2 = []
            for line in fd.readlines():
                items = re.findall(r"[\w']+", line.rstrip())
                for item in items:
                    if item in self._tables:
                        if self._table_data_center[item] == 'dc1':
                            dc1.append(item)
                        else:
                            dc2.append(item)
                        self._table_counts[item] += 1
        self._queries[file] = " ".join(sorted(set(dc1))) + "," + " ".join(sorted(set(dc2)))

    def process_files(self):
        files = glob.glob(os.path.join(self._root_path, "*.sql"))
        for f in files:
            if os.path.isfile(f):
                self.process_file(f)

    def run(self):
        self.process_files()
        if not os.path.exists(self._dest_path):
            os.mkdir(self._dest_path)
        with open(os.path.join(self._dest_path, "queries.csv"), "w") as fd:
            print("query,dc1 tables,dc2 tables", file=fd)
            for q, t in self._queries.items():
                query_text = q.replace("queries/tpcds/", "").replace(".sql", "")
                print(f"{query_text},{t}", file=fd)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("need argument of path to queries")
        exit(1)
    if len(sys.argv) < 3:
        print("need argument of path to results folder")
        exit(1)
    path = sys.argv[1]
    dest = sys.argv[2]
    table_info = QueryInfo(path, dest)
    table_info.run()

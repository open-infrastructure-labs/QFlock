#!/usr/bin/python3
import os
import sys
import re
import glob


class TableInfo:
    table_info = [
        {"name": "store_sales", "datacenter": "dc2"},
        {"name": "catalog_sales", "datacenter": "dc1"},
        {"name": "inventory", "datacenter": "dc2"},
        {"name": "web_sales", "datacenter": "dc1"},
        {"name": "store_returns", "datacenter": "dc2"},
        {"name": "catalog_returns", "datacenter": "dc1"},
        {"name": "web_returns", "datacenter": "dc2"},
        {"name": "customer", "datacenter": "dc1"},
        {"name": "item", "datacenter": "dc2"},
        {"name": "customer_demographics", "datacenter": "dc1"},
        {"name": "customer_address", "datacenter": "dc2"},
        {"name": "date_dim", "datacenter": "dc1"},
        {"name": "time_dim", "datacenter": "dc2"},
        {"name": "catalog_page", "datacenter": "dc1"},
        {"name": "promotion", "datacenter": "dc2"},
        {"name": "household_demographics", "datacenter": "dc1"},
        {"name": "store", "datacenter": "dc2"},
        {"name": "web_site", "datacenter": "dc1"},
        {"name": "call_center", "datacenter": "dc2"},
        {"name": "web_page", "datacenter": "dc1"},
        {"name": "warehouse", "datacenter": "dc2"},
        {"name": "ship_mode", "datacenter": "dc1"},
        {"name": "reason", "datacenter": "dc2"},
        {"name": "income_band", "datacenter": "dc1"}]

    tables = [f['name'] for f in table_info]
    table_data_center = {f['name']: f['datacenter'] for f in table_info}

    def __init__(self, root_path):
        self._root_path = root_path
        self._queries = {}
        self._table_counts = {}
        for t in TableInfo.tables:
            self._table_counts[t] = 0

    def process_file(self, file):
        with open(file, 'r') as fd:
            dc1 = []
            dc2 = []
            for line in fd.readlines():
                items = re.findall(r"[\w']+", line.rstrip())
                for item in items:
                    if item in TableInfo.tables:
                        if TableInfo.table_data_center[item] == 'dc1':
                            dc1.append(item)
                        else:
                            dc2.append(item)
                        self._table_counts[item] += 1
        self._queries[file] = " ".join(sorted(dc1)) + "," + " ".join(sorted(dc2))

    def process_files(self):
        files = glob.glob(os.path.join(self._root_path, "*.sql"))
        for f in files:
            if os.path.isfile(f):
                self.process_file(f)

    def run(self):
        self.process_files()
        print("query,dc1 tables,dc2 tables")
        for q, t in self._queries.items():
            query_text = q.replace("queries/tpcds/", "").replace(".sql", "")
            print(f"{query_text},{t}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("need argument of path to queries")
        exit(1)
    path = sys.argv[1]
    table_info = TableInfo(path)
    table_info.run()

#!/usr/bin/python3
import os
import sys
import re

def process_explain(file):
    with open(file, 'r') as fd:
        queries = {}
        key = None
        for line in fd.readlines():
            if 'query: ' in line:
                items = line.rstrip().split(" ")
                key = items[1].rsplit(os.path.sep, 1)[1]
                #print(f"found key {key} in {line}")
                queries[key] = {'data': [], 'totals': {}}
            if 'QflockLogicalRelation' in line:
                new_line = re.sub(".*QflockLogicalRelation ", "", line)
                #print(line)
                items = new_line.split()
                prev_size = items[0].split(":")[1]
                prev_rows = items[1].split(":")[1]
                size = items[2].split(":")[1]
                rows = items[3].split(":")[1]
                queries[key]['data'].append({'nopush_bytes': int(prev_size),
                                             'push_bytes': int(size),
                                             'nopush_rows': int(prev_rows),
                                             'push_rows': int(rows)})
        return queries

def total_query_data(queries):

    for query in queries:
        queries[query]['totals'] = {'nopush_bytes': 0, 'push_bytes': 0, 'nopush_rows':0, 'push_rows':0}
        for d in queries[query]['data']:
            queries[query]['totals']['nopush_bytes'] += d['nopush_bytes']
            queries[query]['totals']['push_bytes'] += d['push_bytes']
            queries[query]['totals']['nopush_rows'] += d['nopush_rows']
            queries[query]['totals']['push_rows'] += d['push_rows']
            #print(f"{d['nopush_bytes']},{d['push_bytes']},{d['nopush_rows']},{d['push_rows']}")
    return queries

def display_query_data(queries):
    print("query,nopush bytes,push bytes,pct,nopush rows,push rows")
    for query in queries:
        nopush_bytes = queries[query]['totals']['nopush_bytes']
        push_bytes = queries[query]['totals']['push_bytes']
        nopush_rows = queries[query]['totals']['nopush_rows']
        push_rows = queries[query]['totals']['push_rows']
        if nopush_bytes == 0:
            pct_improvement = 0
        else:
            pct_improvement = (nopush_bytes - push_bytes) / nopush_bytes
        print(f"{query},{nopush_bytes},{push_bytes},{pct_improvement:2.5f},{nopush_rows},{push_rows}")
        # print(queries[query])

if __name__ == "__main__":
    query_data = process_explain(sys.argv[1])
    total_query_data(query_data)
    display_query_data(query_data)
#!/usr/bin/python3
import sys
import os

def parse(p_file, output_path):

    if not os.path.exists(output_path):
        os.mkdir(output_path)
    with open(p_file, 'r') as fd:
        queries = []
        for line in fd.readlines():
            if "QflockJdbcPartitionReader: Query complete" in line:
                items = line.rstrip("\n").split(" ")
                query = " ".join(items[6:])
                queries.append(query)
        # Remove duplicates
        queries = list(set(queries))
        query_idx = 0
        for query in queries:
            file_name = os.path.join(output_path, f"{query_idx}.sql")
            with open(file_name, 'w') as fd:
                print(query, file=fd)
            query_idx += 1


if __name__ == "__main__":
    file = sys.argv[1]
    parse(file, output_path="ext_queries")

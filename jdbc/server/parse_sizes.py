#!/usr/bin/python3
import sys

def parse(p_file):
    with open(p_file, 'r') as fd:
        total_queries = 0
        total_filters = 0
        total_est_rows = 0
        total_actual_rows = 0
        queries = {}
        unique_queries = []
        for line in fd.readlines():
            if "query-done" in line:
                items = line.rstrip("\n").split(" ")
                query = " ".join(items[10:])
                cur_rows = int(items[4].split(":")[1])
                est_rows = int(items[5].split(":")[1])
                cur_bytes = int(items[6].split(":")[1])
                est_bytes = int(items[7].split(":")[1])
                nopush_bytes = int(items[8].split(":")[1])
                nopush_rows = int(items[9].split(":")[1])
                total_est_rows += est_rows
                total_actual_rows += cur_rows
                total_queries += 1
                stats = f'{cur_rows},{est_rows},{cur_bytes},{est_bytes},{nopush_rows},{nopush_bytes}'
                if query in queries:
                    queries[query]['count'] += 1
                else:
                    queries[query] = { 'count':1, 'stats':stats }
                    if "WHERE" in query:
                        total_filters += 1
                        unique_queries.append(f'{stats},"{query}"')

        print("Unique queries")
        print("-" * 80)
        print("actual rows,estimate rows,actual bytes,estimate bytes,nopush rows,nopush bytes,query")
        for uq in unique_queries:
            print(uq)
        print(f"queries:{total_queries} filters:{total_filters}")
        print(f"est_rows: {total_est_rows} actual_rows: {total_actual_rows}")

        print("Duplicates")
        print("-"*80)
        print("duplicates,actual rows,estimate rows,actual bytes,estimate bytes,nopush rows,nopush bytes,query")
        for query in queries:
            if queries[query]['count'] > 1:
                print(f'{queries[query]["count"]},{queries[query]["stats"]},"{query}"')



if __name__ == "__main__":
    file = "../volume/logs/jdbc.log"
    if len(sys.argv) > 2:
        file = sys.argv[1]
    parse(file)
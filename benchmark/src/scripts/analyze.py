import csv


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


def load_tables(fname: str):
    tables = dict()
    csvfile = open(fname, newline='')
    reader = csv.DictReader(csvfile, delimiter=',')
    for row in reader:
        t = Table(row)
        tables[t.name] = t

    csvfile.close()
    return tables


class Query:
    def __init__(self, d: dict):
        self.name = d['query']
        self.dc1_table_names = d['dc1 tables'].split()
        self.dc2_table_names = d['dc2 tables'].split()
        self.dc1_tables = dict()
        self.dc2_tables = dict()


def load_queries(fname: str):
    queries = dict()
    csvfile = open(fname, newline='')
    reader = csv.DictReader(csvfile, delimiter=',')
    for row in reader:
        q = Query(row)
        queries[q.name] = q

    csvfile.close()
    return queries


class Result:
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


def load_results(fname: str):
    results = dict()
    csvfile = open(fname, newline='')
    reader = csv.DictReader(csvfile, delimiter=',')
    for row in reader:
        r = Result(row)
        results[r.name] = r

    csvfile.close()
    return results


if __name__ == '__main__':
    tables = load_tables('tables.csv')
    queries = load_queries('queries.csv')
    results = load_results('combined_results.csv')

    for k, q in queries.items():
        for t in q.dc1_table_names:
            q.dc1_tables[t] = tables[t]

        for t in q.dc2_table_names:
            q.dc2_tables[t] = tables[t]

    for k, r in results.items():
        if r.name in queries.keys():
            r.query = queries[r.name]

    spark_time = [r.spark for r in results.values()]
    spark_time.sort(reverse=True)
    # print(spark_time)
    baseline_threshold = spark_time[len(spark_time)//2] # 50 % of results

    gain = [r.gain for r in results.values()]
    gain.sort(reverse=True)
    gain_threshold = gain[len(gain) // 10]  # 10 % of results

    for r in results.values():
        if r.spark < baseline_threshold:
            continue
        if r.gain < gain_threshold:
            continue

        if len(r.query.dc1_table_names) < 2 or len(r.query.dc1_table_names) > 3:
            continue

        if len(r.query.dc2_table_names) < 2 or len(r.query.dc2_table_names) > 3:
            continue

        print(r.name, r.spark, r.jdbc, f'{r.gain*100:.0f}', len(r.query.dc1_table_names),  len(r.query.dc2_table_names))
        q = r.query
        msg = "DC 1 Tables: "
        for t in q.dc1_tables.values():
            msg += f'{t.name}, {t.rows}; '

        print(msg)

        msg = "DC 2 Tables: "
        for t in q.dc2_tables.values():
            msg += f'{t.name}, {t.rows}; '

        print(msg)
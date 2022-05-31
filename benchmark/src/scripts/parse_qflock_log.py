#! /usr/bin/python3
import sys

class ParseQflockLog:

    def __init__(self, qflock_log):
        self._file = qflock_log
        self.log = {}
        self.parse_log()

    def parse_log(self):
        self.log = {}
        with open(self._file, 'r') as fd:
            for line in fd.readlines():
                if "queryName" in line:
                    items = line.split(" ")
                    name = items[0].split(":")[1].replace(".sql", "")
                    app_id_full = items[1].split(":")[1]
                    app_id = app_id_full.split("-")[1]
                    rows = items[2].split(":")[1]
                    bytes = items[3].split(":")[1]
                    table = items[4].split(":")[1]
                    part = items[5].split(":")[1]
                    time_ns = items[6].split(":")[1]
                    query = " ".join(items[7:]).split(":")[1].rsplit("\n")[0]
                    if app_id not in self.log:
                        query_result = {'name': name,
                                 'app_id': app_id,
                                 'queries': {}}
                        self.log[app_id] = query_result
                    else:
                        query_result = self.log[app_id]
                    if app_id_full not in query_result['queries']:
                        query_result['queries'][app_id_full] = {'parts': [],
                        'rows': 0, 'bytes': 0,
                        'query': query, 'table': table}
                    query_result['queries'][app_id_full]['parts'].append({'part': part,
                                                                          'time_ns': time_ns,
                                                                          'query': query,
                                                                          'rows': rows,
                                                                          'bytes': bytes})
                    query_result['queries'][app_id_full]['rows'] += int(rows)
                    query_result['queries'][app_id_full]['bytes'] += int(bytes)

    def show(self):

        for k, v in self.log.items():
            print(f"query name: {v['name']} id: {k} subqueries: {len(v['queries'].keys())}")
            for id, subquery  in v['queries'].items():
                print(f"    {id} - rows: {subquery['rows']} bytes: {subquery['bytes']} " +
                      f"parts: {len(subquery['parts'])} table: {subquery['table']} " +
                      f"query: {subquery['query']}")
                # for part in subquery['parts']:
                #     print(f"        part: {part['part']} time_ns: {part['time_ns']} " +
                #           f"rows: {part['rows']} bytes: {part['bytes']}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        cr = ParseQflockLog(sys.argv[1])
    else:
        cr = ParseQflockLog("data/qflock_log.txt")
    cr.show()

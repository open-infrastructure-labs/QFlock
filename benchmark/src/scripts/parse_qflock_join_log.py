#!/usr/bin/env python3
import sys


class ParseQflockJoinLog:

    def __init__(self, qflock_log):
        self._file = qflock_log
        self.log = {}
        self.parse_log()
        self.log_by_test = {}
        self.get_log_by_test()

    def get_log_by_test(self):
        for query, log in self.log.items():
            if log['name'] in self.log_by_test:
                print(f"get_log_by_test: duplicate test name {log['name']}, dropping")
            else:
                self.log_by_test[log['name']] = log

    def parse_log(self):
        self.log = {}
        with open(self._file, 'r') as fd:
            for line in fd.readlines():
                if "joinStatus" in line:
                    items = line.split(" ")
                    name = items[0].split(":")[1].replace(".sql", "")
                    join_status = items[1].split(":")[1]
                    tables = items[2].split(":")[1]
                    join_type = items[3].split(":")[1]
                    join_hint = items[4].split(":")[1]
                    join_cond = items[5].split(":")[1]
                    logical_plan = " ".join(items[6:]).split(":")[1].rsplit("\n")[0]
                    if name not in self.log:
                        join_info = {'name': name,
                                     'joins': {}}
                        self.log[name] = join_info
                    else:
                        join_info = self.log[name]
                    if tables not in join_info['joins']:
                        join_info['joins'][tables] = {'tables': tables.split(","),
                                                      'plan': "",  # logical_plan,
                                                      'join_status': join_status,
                                                      'join_type': join_type,
                                                      'join_hint': join_hint,
                                                      "join_cond": join_cond,
                                                      }

    def show(self):
        for k, v in self.log.items():
            print(f"query name: {v['name']} id: {k} " +
                  f"joins: {len(v['joins'].keys())} ")
            for tables, join_info in v['joins'].items():
                if True and join_info['join_type'] != "Inner":
                    print(f"    {join_info['tables']} " +
                          f"status: {join_info['join_status']} " +
                          f"type: {join_info['join_type']} " +
                          f"hint: {join_info['join_hint']} " +
                          f"cond: {join_info['join_cond']} ")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        cr = ParseQflockJoinLog(sys.argv[1])
    else:
        cr = ParseQflockJoinLog("data/qflock_log.txt")
    cr.show()

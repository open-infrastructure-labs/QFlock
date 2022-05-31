#! /usr/bin/python3
import sys

class CombinedResult:

    def __init__(self, spark_file, qflock_file):
        self._spark_file = spark_file
        self._qflock_file = qflock_file
        self._spark_results = {}
        self._qflock_results = {}

    def parse_results(self, file):
        results = {}
        with open(file, 'r') as fd:
            for line in fd.readlines():
                if "query" not in line:
                    items = line.split(",")
                    result = {'status': items[0],
                              'rows': items[2],
                              'seconds': items[3]}
                    results[items[1]] = result
        return results

    def gen_combined_result(self):
        with open("data/combined_results.csv", "w") as fd:
            fd.write("query,Jdbc Seconds,Spark Seconds\n")
            for r in self._qflock_results:
                if r in self._spark_results:
                    name = r.replace(".sql", "")
                    fd.write(f"{name},{self._qflock_results[r]['seconds']},"
                             f"{self._spark_results[r]['seconds']}\n")


    def run(self):
        self._spark_results = self.parse_results(self._spark_file)
        self._qflock_results = self.parse_results(self._qflock_file)

        self.gen_combined_result()


if __name__ == "__main__":
    if len(sys.argv) > 2:
        cr = CombinedResult(sys.argv[1], sys.argv[2])
    else:
        cr = CombinedResult("data/spark_results.csv", "data/results.csv")
    cr.run()

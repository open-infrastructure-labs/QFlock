#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import sys

from benchmark.benchmark_stat import BenchmarkStat


class HdfsLogStat(BenchmarkStat):

    def __init__(self, log_file="logs/log.txt"):
        self._total_bytes = 0
        self._file = log_file
        self.header = "DFSClient:bytes"

    def start(self):
        # This stat relies on a log file already being generated ()
        pass

    def end(self):
        if os.path.isfile(self._file):
            self.parse()

    def parse(self):
        with open(self._file, 'r') as fd:
            total = 0
            for line in fd.readlines():
                if "DFSClient readNextPacket" in line:
                    items = line.rstrip("\n").split(" ")
                    cur_bytes = int(items[10].split("=")[1])
                    total += cur_bytes
            self._total_bytes = total

    def __str__(self):
        if self._total_bytes > 0:
            return f"{self._total_bytes}"
        return ""


if __name__ == "__main__":
    file = sys.argv[1]
    stat = HdfsLogStat(file)
    stat.end()
    print(stat)

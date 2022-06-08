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

from benchmark.tpc import TpchBenchmark
from benchmark.tpc import TpcdsBenchmark


class BenchmarkFactory:
    @classmethod
    def get_benchmark(cls, config, sh, verbose=False, no_catalog=False, jdbc=False,
                      qflock_ds=False, test_num=0, results_file="results.csv", results_path="data"):
        if config['benchmark']['db-name'] == "tpch":
            return TpchBenchmark(config['benchmark'], sh, verbose,
                                 no_catalog, jdbc, qflock_ds, test_num, results_file, results_path)
        if config['benchmark']['db-name'] == "tpcds":
            return TpcdsBenchmark(config['benchmark'], sh, verbose,
                                  no_catalog, jdbc, qflock_ds, test_num, results_file, results_path)
        return None

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

import docker
from benchmark.benchmark_stat import BenchmarkStat


class DockerStat(BenchmarkStat):

    def __init__(self):
        self._docker_client = docker.DockerClient(base_url='http://10.124.48.63:2375')
        self._start_bytes = 0
        self._end_bytes = 0

    def start(self):
        self._start_bytes = self._docker_client.containers.get("qflock-storage") \
                                .stats(stream=False)['networks']['eth0']['tx_bytes']

    def end(self):
        self._end_bytes = self._docker_client.containers.get("qflock-storage")\
                              .stats(stream=False)['networks']['eth0']['tx_bytes']

    def __str__(self):
        return f"storage tx_bytes {self._end_bytes - self._start_bytes} "\
               f"{self._end_bytes} {self._start_bytes}"

if __name__ == "__main__":

    ds = DockerStat()
    ds.start()
    ds.end()
    print(ds)
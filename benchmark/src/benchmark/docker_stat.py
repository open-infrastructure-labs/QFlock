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
import logging
from benchmark.benchmark_stat import BenchmarkStat


class DockerStat(BenchmarkStat):
    logged_exception = False
    def __init__(self, container_name, stat_name, adapter):
        try:
            self._docker_client = docker.DockerClient(base_url='http://local-docker-host:2375')
        except BaseException as err:
            if not DockerStat.logged_exception:
                # print(f"Unexpected {err=}, {type(err)=}")
                print("qflock:: Docker stats not available.  Please enable Docker External API Server.")
                DockerStat.logged_exception = True
            self._enabled = False
        else:
            self._enabled = True
        self._start_bytes = 0
        self._end_bytes = 0
        self._container_name = container_name
        self._stat_name = stat_name
        self._adapter = adapter
        self.header = f"{self._container_name}:{self._stat_name}:{self._adapter}"

    def start(self):
        if self._enabled:
            self._start_bytes = self._docker_client.containers.get(self._container_name) \
                                    .stats(stream=False)['networks'][self._adapter][self._stat_name]

    def end(self):
        if self._enabled:
            self._end_bytes = self._docker_client.containers.get(self._container_name)\
                                  .stats(stream=False)['networks'][self._adapter][self._stat_name]

    def __str__(self):
        return f"{self._end_bytes - self._start_bytes} "

    @classmethod
    def get_stats(cls, names):
        stats = []
        for item in names.split(","):
            current_items = item.split(":")
            if len(current_items) == 3:
                container_name, stat_name, adapter = current_items
                stats.append(DockerStat(container_name, stat_name, adapter))
        return stats

if __name__ == "__main__":

    ds = DockerStat()
    ds.start()
    ds.end()
    print(ds)
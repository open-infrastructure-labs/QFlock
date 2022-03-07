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
from glob import glob
import re


class Benchmark:
    """Base class for a benchmark which can:
      - Generate the benchmark data
      - Generate the tables in a database.
      - run specific benchmark tests against the database."""
    def __init__(self, name, config, framework):
        """
        :param name: Name of this benchmark
        :param config: Dict of configuration details
        :param framework: Framework (like spark) used to operate on data.
        """
        self._name = name
        self._config = config
        self._framework = framework

    def generate(self):
        """Generate the database for the given parameters."""
        pass

    @classmethod
    def _get_all_queries(cls, query_path, query_extension):
        all_queries = []
        if os.path.exists(query_path):
            # queries = os.listdir(self._config['query-path'])
            queries = glob(os.path.join(query_path, f"*.{query_extension}"))
            queries = sorted(queries, key=lambda f:
                   int(re.sub("\D+", "",
                       f.rsplit(os.path.sep, 1)[1].rsplit(os.path.extsep, 1)[0]))
                       if re.search("\d", f) else 99999999)

            all_queries.extend(queries)
        return all_queries

    @classmethod
    def get_query_list(cls, query, query_path, query_extension):
        query_list = []
        query_selections = query.split(",")
        for i in query_selections:
            if i == "*":
                query_list.extend(Benchmark._get_all_queries(query_path, query_extension))
            elif "-" in i:
                r = i.split("-")
                if len(r) == 2:
                    for t in range(int(r[0]), int(r[1]) + 1):
                        query_list.append(os.path.join(query_path, str(t)) + "." + query_extension)
            else:
                query_list.append(os.path.join(query_path, str(i)) + "." + query_extension)
        return query_list



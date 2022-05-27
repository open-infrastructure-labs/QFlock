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
    def __init__(self, name, config, framework, tables):
        """
        :param name: Name of this benchmark
        :param config: Dict of configuration details
        :param framework: Framework (like spark) used to operate on data.
        :param tables: Table definitions.
        """
        self._name = name
        self._config = config
        self._framework = framework
        self.tables = tables

    def generate(self):
        """Generate the database for the given parameters."""
        pass

    @classmethod
    def _get_all_queries(cls, query_path, query_extension, exception_list=None,
                         start_query=None, end_query=None, num_queries=None):
        all_queries = []
        query_count = 0
        if os.path.exists(query_path):
            queries = glob(os.path.join(query_path, f"*.{query_extension}"))
            if exception_list:
                new_queries = []
                for q in queries:
                    skip = False
                    for e in exception_list:
                        if re.search(f"\/{e}.sql", q):
                            skip = True
                    if not skip:
                        new_queries.append(q)
                queries = new_queries
            queries = sorted(queries, key=lambda f:
                   int(re.sub("\D+", "",
                       f.rsplit(os.path.sep, 1)[1].rsplit(os.path.extsep, 1)[0]))
                       if re.search("\d", f) else 99999999)
            if start_query:
                new_queries = []
                start_pattern = f"\/{start_query}.sql"
                end_pattern = f"\/{end_query}.sql"
                skip = True
                for q in queries:
                    if re.search(start_pattern, q):
                        skip = False
                    if end_query and re.search(end_pattern, q):
                        new_queries.append(q)
                        break
                    if not skip:
                        new_queries.append(q)
                queries = new_queries

            all_queries.extend(queries)
            if num_queries is not None and len(all_queries) > num_queries:
                all_queries = all_queries[:num_queries]
        return all_queries

    @classmethod
    def get_query_files(cls, query, query_path, query_extension, exception_list=None,
                        num_queries=None):
        query_list = []
        query_count = 0
        files = glob(os.path.join(query_path, str(query)) + "*" + query_extension)
        for file in files:
            skip = False
            if exception_list:
                for e in exception_list:
                    if re.search(f"\/{e}.sql", file):
                        skip = True
            if not skip and (re.search(f"\/{query}.sql", file) or re.search(f"\/{query}[a-z].sql", file)):
                query_list.append(file)
                query_count += 1
            if num_queries is not None and query_count >= num_queries:
                break
        return query_list

    @classmethod
    def get_query_list(cls, query, query_path, query_extension, exceptions=None):
        query_list = []
        num_queries = None
        if ":" in query:
            num_queries = int(query.split(":")[1])
            query = query.split(":")[0]
        query_selections = query.split(",")
        for i in query_selections:
            if i == "*":
                query_list.extend(Benchmark._get_all_queries(query_path, query_extension, exceptions,
                                                             num_queries=num_queries))
            elif "-*" in i:
                r = i.split("-")
                if len(r) == 2:
                    query_list.extend(Benchmark._get_all_queries(query_path, query_extension, exceptions,
                                                                 start_query=r[0], num_queries=num_queries))
            elif "-" in i:
                r = i.split("-")
                if len(r) == 2:
                    query_list.extend(Benchmark._get_all_queries(query_path, query_extension, exceptions,
                                                                 start_query=r[0], end_query=r[1],
                                                                 num_queries=num_queries))
            else:
                qf = Benchmark.get_query_files(str(i), query_path, query_extension, exceptions, num_queries)
                query_list.extend(qf)
        return query_list



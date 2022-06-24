#!/usr/bin/env python3
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
import sys
import subprocess
import time

#
# The purpose of this script is to launch our qflock-bench.py in a docker.
# In some cases we also need to re-quote arguments whose quotes were stripped by python.
#
if __name__ == "__main__":
    needs_quotes_args = ['--queries', '--query_text', '--query_range', '--view_columns']
    arg_string = ""
    argc = len(sys.argv)
    i = 1
    while i < argc:
        arg_string += f"{sys.argv[i]} "
        if sys.argv[i] in needs_quotes_args:
            i += 1
            if i < argc:
                arg_string += f'"{sys.argv[i]}" '
        i += 1
    cmd = "docker exec -it qflock-spark-dc1 ./qflock-bench.py " + arg_string
    print(cmd)
    start_time = time.time()
    status = subprocess.call(cmd, shell=True)
    delta_time = time.time() - start_time
    print(f"total seconds: {delta_time:3.2f}")
    if status != 0:
        print(f"\n*** Exit code was {status}")
        exit(status)
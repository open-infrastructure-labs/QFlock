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
from benchmark.util import run_command
import subprocess

class SparkLauncher:
    def __init__(self, config):
        self._config = config

    # "venv/bin/spark-submit --master local[1] --conf "spark.driver.memory=2g" ./bench.py -f"
    # bin/pyspark --conf spark.sql.catalogImplementation=hive --packages org.apache.spark:spark-hive_2.12:3.2.0,mysql:mysql-connector-java:8.0.28 --conf spark.sql.extensions=com.github.datasource.generic.FederationExtensions --jars pushdown-datasource_2.12-0.1.0.jar

    def get_spark_cmd(self, cmd, workers):
        spark_cmd = f'spark-submit --master {self._config["master"]} '
        if workers != None and workers > 0:
            spark_cmd += f"--total-executor-cores {workers} "
        spark_cmd += " ".join([f'--conf \"{arg}\" ' for arg in self._config["conf"]])
        if "packages" in self._config:
            spark_cmd += " --packages " + ",".join([arg for arg in self._config["packages"]])
        if "jars" in self._config:
            spark_cmd += " --jars " + ",".join([arg for arg in self._config["jars"]])
        # if "debug_options" in self._config:
        #     debug = self._config['debug_options']
        #     spark_cmd += f'extraJavaOptions="{debug["classpath"]} {debug["agentlib"]}{debug["address"]}"'
        spark_cmd += f" {cmd}"
        return spark_cmd

    def spark_submit(self, command, workers=None, enable_stdout=False, wait_text=None):
        spark_cmd = self.get_spark_cmd(command, workers=workers)
        print("*" * 30)
        print(spark_cmd)
        print("*" * 30)
        if enable_stdout:
            rc = subprocess.call(spark_cmd, shell=True)
            return rc, ""
        else:
            # result = subprocess.run(spark_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            #                         shell=True)
            # return result.returncode, result.stdout
            output_lines = []
            enable_tracing = False
            process = subprocess.Popen(
                spark_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)  # shlex.split(command)
            while True:
                output = process.stdout.readline()
                if (not output or output == '') and process.poll() is not None:
                    break
                if output and wait_text:
                    output_text = str(output, 'utf-8')
                    if enable_tracing:
                        print(output_text.rstrip('\n'))
                    elif wait_text in output_text:
                        enable_tracing = True
                output_lines.append(output_text)
            rc = process.poll()
            return rc, output_lines


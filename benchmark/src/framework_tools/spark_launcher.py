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
import subprocess
from benchmark.config import Config

class SparkLauncher:
    def __init__(self, config, args):
        self._config = config
        self._args = args
        self._metastore_ports = Config.get_metadata_ports(self._config)

    def filter_config(self, conf):
        if "agentlib" in conf and ("debug" not in self._args or not self._args.debug):
            return False
        else:
            return True

    def get_spark_cmd(self, cmd, workers):
        spark_cmd = f'spark-submit --master {self._config["master"]} '
        if workers is not None and workers > 0:
            spark_cmd += f"--total-executor-cores {workers} "
        conf = list(filter(self.filter_config, self._config["conf"]))
        spark_cmd += " ".join([f'--conf \"{arg}\" ' for arg in conf])
        if "packages" in self._config:
            spark_cmd += " --packages " + ",".join([arg for arg in self._config["packages"]])
        if "jars" in self._config:
            spark_cmd += " --jars " + ",".join([arg for arg in self._config["jars"]])
        if self._args.catalog_name:
            spark_cmd += f" --conf spark.hadoop.hive.metastore.uris=thrift://{self._config['hive-metastore']}" +\
                         f":{self._metastore_ports[self._args.catalog_name]}"
        spark_cmd += f" {cmd}"
        return spark_cmd

    def spark_submit(self, command, workers=None, enable_stdout=False, wait_text=None,
                     logfile="logs/log.txt"):
        # print(f"spark_submit:: workers {workers} enable_stdout {enable_stdout} wait_text {wait_text}" +
        #       f" logfile {logfile} terse {self._args.terse}")
        spark_cmd = self.get_spark_cmd(command, workers=workers)
        if not self._args.terse:
            print("*" * 30)
            print(spark_cmd)
            print("*" * 30)
        fd = None
        if logfile:
            fd = open(logfile, 'w')
        if enable_stdout and not logfile:
            # rc = subprocess.call(spark_cmd, shell=True)
            # return rc, ""
            result = subprocess.run(spark_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                    shell=True)
            return result.returncode, result.stdout
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
                if output:
                    output_text = str(output, 'utf-8')
                    if logfile:
                        print(output_text.rstrip('\n'), file=fd)
                    if 'qflock:: ' in output_text:
                        print(output_text.rstrip('\n').lstrip('qflock:: '))
                    elif wait_text is not None:
                        if enable_tracing:
                            print(output_text.rstrip('\n'))
                        elif wait_text in output_text:
                            enable_tracing = True
                    elif enable_stdout:
                        print(output_text.rstrip('\n'))
                    output_lines.append(output_text)
            rc = process.poll()
            return rc, output_lines

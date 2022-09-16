#!/usr/bin/python3
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
import json
import subprocess
import os
import sys
import logging
import argparse
from argparse import RawTextHelpFormatter
import yaml

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from com.github.qflock.jdbc.api import QflockJdbcService
from thrift_handler import QflockThriftJdbcHandler


class QflockJdbcServer:
    """Runs a server with a JDBC API."""
    def __init__(self):
        self._config = None
        self._args = None
        self._parse_args()
        self._init_config()

    def _parse_args(self):
        parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter,
                                         description="JDBC Server.\n")
        parser.add_argument("--file", "-f", default="config.yaml",
                            help="config .yaml file to use")
        parser.add_argument("--mode", default="spark-submit",
                            help="mode to launch in (spark-submit, local)")
        parser.add_argument("--debug_spark", action="store_true",
                            help="allow debug of spark server")
        parser.add_argument("--debug_pyspark", action="store_true",
                            help="allow debug of pyspark")
        self._args = parser.parse_args()

    def _init_config(self):
        config_file = self._args.file
        if not os.path.exists(config_file):
            print(f"{config_file} is missing.  Please add it.")
            exit(1)
        with open(config_file, "r") as fd:
            try:
                self._config = yaml.safe_load(fd)
            except yaml.YAMLError as err:
                print(err)
                print(f"{config_file} is missing.  Please add it.")
                exit(1)

    def get_jdbc_ip(self):
        if self._args.mode == "local" or self._args.debug_spark:
            return self._config['server-name']

        # The below gets the IP for debugging.
        result = subprocess.run('docker network inspect qflock-net'.split(' '), stdout=subprocess.PIPE)
        d = json.loads(result.stdout)

        return d[0]['IPAM']['Config'][0]['Gateway']

    def setup_logger(self):
        if self._config['log-level'] == "DEBUG":
            log_level = logging.DEBUG
        elif self._config['log-level'] == "WARN":
            log_level = logging.WARNING
        elif self._config['log-level'] == "ERROR":
            log_level = logging.ERROR
        else:
            log_level = logging.INFO

        formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s %(message)s',
                                      '%Y-%m-%d %H:%M:%S')
        # ch.setFormatter(formatter)
        logging.basicConfig(level=log_level,
                            format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')
        root = logging.getLogger()
        hdlr = root.handlers[0]
        hdlr.setFormatter(formatter)
        logging.info("Logger Configured")

    def serve(self):
        self.setup_logger()
        jdbc_port = self._config['server-port']
        jdbc_ip = self.get_jdbc_ip()
        logging.info(f"Starting JDBC Server ip: {jdbc_ip}:{jdbc_port}")
        handler = QflockThriftJdbcHandler(spark_log_level=self._config['spark']['log-level'],
                                          metastore_ip=self._config['spark']['hive-metastore'],
                                          metastore_port=self._config['spark']['hive-metastore-port'],
                                          debug_pyspark=self._args.debug_pyspark,
                                          compression=self._config['compression'])
        processor = QflockJdbcService.Processor(handler)
        transport = TSocket.TServerSocket(host=jdbc_ip, port=jdbc_port)
        tfactory = TTransport.TBufferedTransportFactory()
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()

        # jdbc_server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
        jdbc_server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
        logger = logging.getLogger("qflock")
        logger.info(f'Starting the Qflock JDBC server...{jdbc_ip}:{jdbc_port} '
                    f'spark log-level:{self._config["log-level"]} compression:{self._config["compression"]}')
        try:
            jdbc_server.serve()
        except BaseException as ex:
            pass

    def filter_config(self, conf):
        if "agentlib" in conf and (not self._args.debug_spark):
            return False
        else:
            return True

    def get_spark_cmd(self, cmd):
        spark_conf = self._config['spark']
        spark_cmd = f'spark-submit --master {spark_conf["master"]} '
        workers = spark_conf.get("workers")
        if workers is not None and int(workers) > 0:
            spark_cmd += f"--total-executor-cores {workers} "
        print(spark_conf["conf"])
        # Filter out any conf items that are not enabled by arguments.
        conf = list(filter(self.filter_config, spark_conf["conf"]))
        spark_cmd += " ".join([f'--conf \"{arg}\" ' for arg in conf])
        if "packages" in spark_conf and spark_conf["packages"]:
            spark_cmd += " --packages " + ",".join([arg for arg in spark_conf["packages"]])
        if "jars" in spark_conf and len(spark_conf["jars"]):
            spark_cmd += " --jars " + ",".join([arg for arg in spark_conf["jars"]])
        spark_cmd += f" --conf spark.hadoop.hive.metastore.uris=thrift://{spark_conf['hive-metastore']}" +\
                     f":{spark_conf['hive-metastore-port']}"
        spark_cmd += f" {cmd}"
        return spark_cmd

    def spark_submit(self):
        spark_cmd = self.get_spark_cmd(f"{sys.argv[0]} --mode local")
        print(spark_cmd)
        logging.info(80*"*")
        logging.info(spark_cmd)
        logging.info(80*"*")
        subprocess.call(spark_cmd, shell=True)

    def run(self):
        if self._args.mode == 'spark-submit':
            self.spark_submit()
        else:
            self.serve()


if __name__ == '__main__':
    server = QflockJdbcServer()
    server.run()

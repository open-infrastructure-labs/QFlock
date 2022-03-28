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
import yaml


class Config:
    def __init__(self, file):
        self._file = file
        self._config = self._load_config()

    @classmethod
    def get_metadata_ports(cls, config):
        port_map = {}
        if 'hive-metastore-ports' in config:
            ports = config["hive-metastore-ports"].split(',')
            for catalog, port in [port.split(':') for port in ports]:
                port_map[catalog] = port
        return port_map

    def __getitem__(self, arg):
        return self._config[arg]

    def _load_config(self):
        config_file = self._file
        if not os.path.exists(config_file):
            print(f"{config_file} is missing.  Please add it.")
            exit(1)
        with open(config_file, "r") as fd:
            try:
                return yaml.safe_load(fd)
            except yaml.YAMLError as err:
                print(err)
                print(f"{config_file} is missing.  Please add it.")
                exit(1)

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
import sys
import subprocess
import logging


def run_command(command, show_cmd=False,
                enable_stdout=True, no_capture=False,
                log_file="", debug=False, dry_run=False,
                wait_for_string=None):
    output_lines = []
    log_fd = None
    if os.path.exists(log_file):
        mode = "a"
    else:
        mode = "w"
    if log_file != "":
        logging.info("opening {}".format(log_file))
        log_fd = open(log_file, mode)
    if show_cmd or debug:
        logging.info("{}: {} ".format(sys.argv[0], command))
    if dry_run:
        # logging.info("")
        return 0, output_lines
    if wait_for_string:
        enable_stdout = False
    if no_capture and wait_for_string == None:
        rc = subprocess.call(command, shell=True)
        return rc, output_lines
    process = subprocess.Popen(
        command.split(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        universal_newlines=True)
    while True:
        line = process.stdout.readline()
        if (not line or line == '') and process.poll() is not None:
            break
        if wait_for_string and wait_for_string in line:
            enable_stdout = True
        if line and enable_stdout:
            sys.stdout.write(line.strip() + "\n")
        if line and log_fd:
            log_fd.write(line.strip() + "\n")
        output_lines.append(line)
    if log_fd:
        log_fd.close()
    rc = process.poll()
    return rc, output_lines

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


def shell_cmd(cmd, show_cmd=False, fail_on_err=True,
              err_msg=None, enable_stdout=True, no_capture=False):
    rc, output = run_cmd(
        cmd, show_cmd, enable_stdout=enable_stdout, no_capture=no_capture)
    if fail_on_err and rc != 0:
        logging.info("cmd failed with status: {} cmd: {}".format(rc, cmd))
        if err_msg:
            logging.info(err_msg)
    return rc, output

def run_cmd(command, show_cmd=False, enable_stdout=True, no_capture=False, log_file=""):
    output_lines = []
    log_fd = None
    if os.path.exists(log_file):
        mode = "a"
    else:
        mode = "w"
    if log_file != "":
        logging.info("opening {}".format(log_file))
        log_fd = open(log_file, mode)
    if show_cmd:
        logging.info("{}: {} ".format(sys.argv[0], command))
    if no_capture:
        rc = subprocess.call(command, shell=True)
        return rc, output_lines
    process = subprocess.Popen(
        command.split(), stdout=subprocess.PIPE)  # shlex.split(command)
    while True:
        output = process.stdout.readline()
        if (not output or output == '') and process.poll() is not None:
            break
        if output and enable_stdout:
            print(str(output, 'utf-8').strip())
        if output and log_fd:
            log_fd.write(str(output, 'utf-8').strip() + "\n")
        output_lines.append(str(output, 'utf-8'))
    if log_fd:
        log_fd.close()
    rc = process.poll()
    return rc, output_lines
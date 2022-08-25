/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.qflock.server

import org.apache.log4j.BasicConfigurator

object ServerTest {

  def main(args: scala.Array[String]): Unit = {
    val mode = if (args.length > 0) {
      args(0)
    } else "server"
    if (mode == "server") {
      // BasicConfigurator.configure
      val server = new Server("qflock-jdbc-dc2", 9860)
      server.start()
    } else {

    }
  }
}

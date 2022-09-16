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
package com.github.qflock.extensions.jdbc

import java.io.FileWriter

object QflockLog {
  private var defaultPath = "data"
  def setPath(defPath: String): Unit = defaultPath = defPath
  def log(message: String, path: String = defaultPath): Unit = {
    this.synchronized {
      val fw = new FileWriter(s"$path/qflock_log.txt", true)
      try {
        fw.write(message + "\n")
      }
      finally fw.close()
    }
  }
}

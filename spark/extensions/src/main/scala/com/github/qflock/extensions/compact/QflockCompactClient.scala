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
package com.github.qflock.extensions.compact

import java.io.{BufferedInputStream, DataInputStream, StringWriter}
import java.net.{HttpURLConnection, URL}
import java.util
import javax.json.Json

import org.apache.spark.sql.types.StructType


class QflockCompactClient(options: util.Map[String, String]) {
  private def getJson(query: String): String = {
    val queryBuilder = Json.createObjectBuilder()
    queryBuilder.add("query", query)
    val queryJson = queryBuilder.build
    val stringWriter = new StringWriter
    val writer = Json.createWriter(stringWriter)
    writer.writeObject(queryJson)
    stringWriter.getBuffer().toString()
  }

  def getQueryStream(query: String, schema: StructType): DataInputStream = {
    val url = new URL(options.get("url"))
    val con = url.openConnection.asInstanceOf[HttpURLConnection]
    con.setRequestMethod("POST")
    con.setRequestProperty("Accept", "application/json")
    con.setDoOutput(true)
    con.setReadTimeout(0)
    con.setConnectTimeout(0)
    val jsonString = getJson(query)
    val os = con.getOutputStream
    try {
      val input = jsonString.getBytes("utf-8")
      os.write(input, 0, input.length)
    } finally if (os != null) os.close()
    new DataInputStream(new BufferedInputStream(con.getInputStream))
  }
}

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
package org.apache.spark.sql.hive.extension

import java.lang.{Iterable => JIterable}
import java.util.{Locale, Map => JMap}

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql._


object ExtHiveUtils extends Logging {
  private val sparkSession: SparkSession = SparkSession
    .builder()
    .getOrCreate()
  private val hiveClient = {
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    val hiveConf = newHiveConf(sparkSession.sparkContext.getConf,
      hadoopConf)
    new HiveMetaStoreClient(hiveConf)
  }
  def newHiveConf(sparkConf: SparkConf,
                  hadoopConf: JIterable[JMap.Entry[String, String]],
                  classLoader: Option[ClassLoader] = None): HiveConf = {
    val hiveConf = new HiveConf(classOf[SessionState])
    // HiveConf is a Hadoop Configuration, which has a field of classLoader and
    // the initial value will be the current thread's context class loader.
    // We call hiveConf.setClassLoader(initClassLoader) at here to ensure it use the classloader
    // we want.
    classLoader.foreach(hiveConf.setClassLoader)
    // 1: Take all from the hadoopConf to this hiveConf.
    // This hadoopConf contains user settings in Hadoop's core-site.xml file
    // and Hive's hive-site.xml file. Note, we load hive-site.xml file manually in
    // SharedState and put settings in this hadoopConf instead of relying on HiveConf
    // to load user settings. Otherwise, HiveConf's initialize method will override
    // settings in the hadoopConf. This issue only shows up when spark.sql.hive.metastore.jars
    // is not set to builtin. When spark.sql.hive.metastore.jars is builtin, the classpath
    // has hive-site.xml. So, HiveConf will use that to override its default values.
    // 2: we set all spark confs to this hiveConf.
    // 3: we set all entries in config to this hiveConf.
    val confMap = (hadoopConf.iterator().asScala.map(kv => kv.getKey -> kv.getValue) ++
      sparkConf.getAll.toMap).toMap
    confMap.foreach { case (k, v) => hiveConf.set(k, v) }
    // Disable CBO because we removed the Calcite dependency.
    hiveConf.setBoolean("hive.cbo.enable", false)
    hiveConf
  }
  def getTable(dbName: String, tableName: String): Table = {
    hiveClient.getTable(dbName, tableName)
  }
  def getAllTables(dbName: String): Array[String] = {
    hiveClient.getAllTables(dbName).asScala.toArray[String]
  }
  def getDatabases(): Array[String] = {
    hiveClient.getAllDatabases.asScala.toArray[String]
  }
}


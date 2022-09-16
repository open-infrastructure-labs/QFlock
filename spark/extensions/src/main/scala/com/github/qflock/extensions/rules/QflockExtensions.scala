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
package com.github.qflock.extensions.rules

import java.io.FileWriter

import org.slf4j.{Logger, LoggerFactory}

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

case class QflockBasicRule(spark: SparkSession) extends Rule[LogicalPlan] {
  protected val logger: Logger = LoggerFactory.getLogger(getClass)
  override def apply(plan: LogicalPlan): LogicalPlan = {
    logger.info(s"QflockRule LogicalPlan $plan")
    val fw = new FileWriter("./rules.txt", false)
    try {
      fw.write(plan.toString() + "\n")
    }
    finally fw.close()
    plan
  }
}

class QflockExplainExtensions extends (SparkSessionExtensions => Unit) {
  def apply(e: SparkSessionExtensions): Unit = {
    e.injectOptimizerRule(QflockExplainRule)
  }
}

class QflockExtensions extends (SparkSessionExtensions => Unit) {
  def apply(e: SparkSessionExtensions): Unit = {
    e.injectOptimizerRule(QflockRemoteRule)
  }
}

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
package com.github.qflock.extensions.remote

import org.apache.spark.sql.connector.write._

/** Is a basic commit message required by the DataWriter api.
 *
 * @param partId current partition
 * @param taskId current task
 */
class QflockRemoteWriterCommitMessage(partId: Int, taskId: Long) extends WriterCommitMessage {

  def getPartitionId: Int = partId
  override def equals(obj: Any): Boolean = {
    if (this == obj) {
      true
    } else {
      if (!obj.isInstanceOf[QflockRemoteWriterCommitMessage]) {
        false
      } else {
        val msg = obj.asInstanceOf[QflockRemoteWriterCommitMessage]
        partId == msg.getPartitionId
      }
    }
  }
  override def hashCode: Int = partId

  override def toString: String =
    "QflockRemoteWriterCommitMessage(" + "partitionId=" + partId + " taskId=" + taskId + ')'

  def getTaskId: Long = taskId
}

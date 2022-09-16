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

import java.nio.ByteBuffer

/** This defines the layout of the Qflock Server's Header.
 *
 */
object QflockServerHeader {
  /** This encodes the offset of the fields in the header.
   */
  object Offset {
    val dataType: Int = 0 * 4
    val typeSize: Int = 1 * 4
    val dataLen: Int = 2 * 4
    val compressedLen: Int = 3 * 4
  }
  object Length {
    val Long: Int = 8
    val Integer: Int = 4
    val Double: Int = 8
  }
  val bytes: Int = 4 * 4
  val stringLength: Int = 120
  val magic: Int = 42424242
  val batchSize: Int = 4 * 1024 * 1024
  val streamTerminator: Array[Byte] = {
    val byteBuffer = ByteBuffer.allocate(4 * 4)
    for (_ <- Range(0, 4)) {
      byteBuffer.putInt(0)
    }
    byteBuffer.array()
  }
  /** Type of object encoded in binary.
   *  This follows the encoding values used by the NDP server.
   */
  object DataType extends Enumeration {
    type DataType = Value
    val LongType: QflockServerHeader.DataType.Value = Value(1)
    val DoubleType: QflockServerHeader.DataType.Value = Value(2)
    val ByteArrayType: QflockServerHeader.DataType.Value = Value(3)
    val FixedLenByteArrayType: QflockServerHeader.DataType.Value = Value(4)
  }
}

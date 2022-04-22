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
package com.github.qflock.datasource.hdfs

import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.URI
import java.util

import com.github.qflock.datasource.common.Pushdown
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.BlockLocation
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import org.apache.spark.sql.SparkSession

/** A Factory to fetch the correct type of
 *  store object.
 */
object HdfsStoreFactory {
  /** Returns the store object.
   *
   * @param pushdown object handling filter, project and aggregate pushdown
   * @param options the parameters including those to construct the store
   * @return a new HdfsStore object constructed with above parameters.
   */
  def getStore(pushdown: Pushdown,
               options: java.util.Map[String, String],
               sparkSession: SparkSession,
               sharedConf: Configuration):
      HdfsStore = {
    new HdfsStore(pushdown, options, sparkSession, sharedConf)
  }
}
/** A hdfs store object which can connect
 *  to a file on hdfs filesystem, specified by options("path"),
 *  And which can read a partition with any of various pushdowns.
 *
 * @param pushdown object handling filter, project and aggregate pushdown
 * @param options the parameters including those to construct the store
 */
class HdfsStore(pushdown: Pushdown,
                options: java.util.Map[String, String],
                sparkSession: SparkSession,
                sharedConf: Configuration) {
  override def toString() : String = "HdfsStore" + options + pushdown.filters.mkString(", ")
  protected val path = options.get("path")
  protected val isPushdownNeeded: Boolean = pushdown.isPushdownNeeded
  protected val endpoint = {
    val server = path.split("/")(2)
    if (path.contains("webhdfs://")) {
      ("webhdfs://" + server + {if (server.contains(":9870")) "" else ":9870"})
    } else {
      ("hdfs://" + server + {if (server.contains(":9000")) "" else ":9000"})
    }
  }
  val filePath = {
    val server = path.split("/")(2)
    if (path.contains("webhdfs")) {
      path.replace(server, server + {if (server.contains(":9870")) "" else ":9870"})
    } else {
      path.replace(server, server + {if (server.contains(":9000")) "" else ":9000"})
    }
  }
  protected val logger = LoggerFactory.getLogger(getClass)
  protected val configure: Configuration = {
    /* val conf = new Configuration()
    conf.set("dfs.datanode.drop.cache.behind.reads", "true")
    conf.set("dfs.client.cache.readahead", "0") */
    sharedConf
  }
  protected val fileSystem = {
    val conf = configure
    FileSystem.get(URI.create(endpoint), conf)
  }
  protected val fileSystemType = fileSystem.getScheme
  protected val traceReadable: Boolean =
    (!options.containsKey("DisableCasts") && !options.containsKey("useColumnNames"))

  /** Returns a list of BlockLocation object representing
   *  all the hdfs blocks in a file.
   *
   * @param fileName the full filename path
   * @return Map[String, BlockLocation] The Key is the filename
   *                     the value is the Array of BlockLocation
   */
  def getBlockList(fileName: String) : scala.collection.immutable.Map[String,
    Array[BlockLocation]] = {
    val fileToRead = new Path(fileName)
    val fileStatus = fileSystem.getFileStatus(fileToRead)
    val blockMap = scala.collection.mutable.Map[String, Array[BlockLocation]]()
    if (fileSystem.isFile(fileToRead)) {
      // Use MaxValue to indicate we want info on all blocks.
      blockMap(fileName) = fileSystem.getFileBlockLocations(fileToRead, 0, Long.MaxValue)
    } else {
      /* fileToRead is a directory. So get the contents of this directory.
       * For each file in the directory create a new map entry with filename as key.
       */
      val status = fileSystem.listStatus(fileToRead)
      for (item <- status) {
        if (item.isFile && (item.getPath.getName.contains(".csv") ||
                            item.getPath.getName.contains(".tbl") ||
                            item.getPath.getName.contains(".parquet"))) {
          val currentFile = item.getPath.toString
          // Use MaxValue to indicate we want info on all blocks.
          blockMap(currentFile) = fileSystem.getFileBlockLocations(item.getPath, 0, Long.MaxValue)
        }
      }
    }
    blockMap.toMap
  }

  /** Returns the length of the file in bytes.
   *
   * @param fileName the full path of the file
   * @return byte length of the file.
   */
  def getLength(fileName: String) : Long = {
    val fileToRead = new Path(fileName)
    val fileStatus = fileSystem.getFileStatus(fileToRead)
    fileStatus.getLen
  }

  /** Returns the last modification time of the file.
   *
   * @param fileName the full path of the file
   * @return modified time of the file.
   */
  def getModifiedTime(fileName: String) : Long = {
    val fileToRead = new Path(filePath)
    val fileStatus = fileSystem.getFileStatus(fileToRead)
    // logger.info("fileStatus {}", fileStatus.toString)
    fileStatus.getModificationTime
  }
  /** Returns the offset, length in bytes of an hdfs partition.
   *  This takes into account any prior lines that might be incomplete
   *  from the prior partition.
   *
   * @param partition the partition to find start for
   * @return (offset, length) - Offset to begin reading partition, Length of partition.
   */
  @throws(classOf[Exception])
  def getPartitionInfo(partition: HdfsPartition) : (Long, Long) = {
    val currentPath = new Path(partition.name)
    var startOffset = partition.offset
    var nextChar: Integer = 0
    if (partition.offset != 0) {
      /* Scan until we hit a newline. This skips the (normally) partial line,
       * which the prior partition will read, and guarantees we get a full line.
       * The only way to guarantee full lines is by reading up to the line terminator.
       */
      val inputStream = fileSystem.open(currentPath)
      inputStream.seek(partition.offset)
      val reader = new BufferedReader(new InputStreamReader(inputStream))
      do {
        nextChar = reader.read
        startOffset += 1
      } while ((nextChar.toChar != '\n') && (nextChar != -1));
    }
    var partitionLength = (partition.offset + partition.length) - startOffset
    if (!partition.last) {
     /* Scan up to the next line after the end of the partition.
      * We always include this next line to ensure we are reading full lines.
      * The only way to guarantee full lines is by reading up to the line terminator.
      */
      val inputStream = fileSystem.open(currentPath)
      inputStream.seek(partition.offset + partition.length)
      val reader = new BufferedReader(new InputStreamReader(inputStream))
      do {
        nextChar = reader.read
        // Only count the char if we are not at end of line.
        if (nextChar != -1) {
          partitionLength += 1
        }
      } while ((nextChar.toChar != '\n') && (nextChar != -1));
    }
    (startOffset, partitionLength)
  }
  /** The kind of header we should use.
   *  In our case we only ever use None or Ignore, since
   *  we use casts and column numbers, so the header is not needed.
   * @return NONE or IGNORE
   */
  def headerType(): String = {
    /* If we do not push down, then we will read from hdfs directly,
     * and therefor need to skip the header ourselves.
     * When we use ndp, it skips the header for us since we tell it about the header.
     */
    if (options.containsKey("header") && (options.get("header") == "true")) {
      "IGNORE"
    } else {
      "NONE"
    }
  }
  /** Returns true if we should skip the header.
   *
   * @param partition the partition to read
   * @return true to skip the header and false otherwise.
   */
  def skipHeader(partition: HdfsPartition): Boolean = {
    /* If we do not push down, then we will read from hdfs directly,
     * and therefor need to skip the header ourselves.
     * When we use ndp, it skips the header for us since we tell it about the header.
     */
    if (options.containsKey("header") && (options.get("header") == "true")) {
      (partition.index == 0 && !isPushdownNeeded)
    } else {
      false
    }
  }
}

/** Related routines for the HDFS connector.
 *
 */
object HdfsStore {

  protected val logger = LoggerFactory.getLogger(getClass)
  private val sparkSession: SparkSession = SparkSession
      .builder()
      .getOrCreate()

  /** Returns true if pushdown is supported by this flavor of
   *  filesystem represented by a string of "filesystem://filename".
   *
   * @param options map containing "path".
   * @return true if pushdown supported, false otherwise.
   */
  def pushdownSupported(options: util.Map[String, String]): Boolean = {
    if (options.get("format").contains("parquet")) {
      // Regular hdfs does pushdown in the datasource.
      true
    } else {
      // Other filesystems like hdfs and webhdfs do not support pushdown.
      false
    }
  }

  /** Returns true if a filter can be fully pushed down.  This occurs when
   *  the entity we pushdown to guarantees that the filter does not need
   *  to be re-evaluated on the results returned.
   *  It is worth noting that if we cannot fully pushdown filters, then
   *  Spark will not consider pushing down aggregates.
   *
   * @param options map containing "path".
   * @return true if pushdown supported, false otherwise.
   */
  def filterPushdownFullySupported(options: util.Map[String, String]): Boolean = {
    if (options.get("path").startsWith("hdfs://") &&
        options.get("format").contains("parquet")) {
      // With parquet, the filters need to be re-evaluated.
      false
    } else {
      // everything else supports full pushdown
      true
    }
  }
  /** returns a string with the pathname of the hdfs object,
   *  including the server port.  The user might not include the port,
   *  and if it is missing, we will add it.
   *
   *  @param filePath the hdfs filename.
   * @return String the new file path in hdfs format with server port.
   */
  def getFilePath(filePath: String): String = {
    val server = filePath.split("/")(2)
    if (filePath.contains("webhdfs")) {
      filePath.replace(server, server + {if (filePath.contains(":9870")) "" else ":9870"})
    } else {
      filePath.replace(server, server + {if (filePath.contains(":9000")) "" else ":9000"})
    }
  }
  /** Returns a list of file names represented by an input file or directory.
   *
   * @param fileName the full filename path
   * @return Map[String, BlockLocation] The Key is the filename
   *                     the value is the Array of BlockLocation
   */
  def getFileList(fileName: String, fileSystem: FileSystem):
      Seq[String] = {
    val fileToRead = new Path(getFilePath(fileName))
    val fileStatus = fileSystem.getFileStatus(fileToRead)
    var fileArray: Array[String] = Array[String]()
    if (fileSystem.isFile(fileToRead)) {
      // Use MaxValue to indicate we want info on all blocks.
      fileArray = fileArray ++ Array(fileName)
    } else {
      /* fileToRead is a directory. So get the contents of this directory.
       * For each file in the directory create a new map entry with filename as key.
       */
      val status = fileSystem.listStatus(fileToRead)
      for (item <- status) {
        if (item.isFile && (item.getPath.getName.contains(".csv") ||
                            item.getPath.getName.contains(".tbl") ||
                            item.getPath.getName.contains(".parquet"))) {
          val currentFile = item.getPath.toString
          fileArray = fileArray ++ Array(currentFile)
        }
      }
    }
    fileArray.toSeq
  }
  private val configuration: Configuration = {
    val conf = sparkSession.sessionState.newHadoopConf()
    // conf.set("dfs.datanode.drop.cache.behind.reads", "true")
    // conf.set("dfs.client.cache.readahead", "0")
    // conf.set("fs.ndphdfs.impl", classOf[org.dike.hdfs.NdpHdfsFileSystem].getName)
    // conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName())
    // conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())
    conf
  }
  /** Extracts the endpoint, aka server name and port from the file path.
   *
   *  @param path The pathname to extract the endpoint from.
   *  @return String the endpoint.
   */
  private def getEndpoint(path: String): String = {
    val server = path.split("/")(2)
    if (path.contains("webhdfs://")) {
      ("webhdfs://" + server + {if (path.contains(":9870")) "" else ":9870"})
    } else {
      ("hdfs://" + server + {if (path.contains(":9000")) "" else ":9000"})
    }
  }
  /** Fetches the FileSystem for this file string
   *  @param filePath the file pathname to get the hdfs filesystem for.
   *  @return FileSystem
   */
  def getFileSystem(filePath: String): FileSystem = {
    val conf = configuration
    getFileSystem(filePath, conf)
  }
  /** Fetches the FileSystem for this file string
   *  @param filePath the file pathname to get the hdfs filesystem for.
   *  @param conf the hdfs configuration.
   *  @return FileSystem
   */
  def getFileSystem(filePath: String, conf: Configuration): FileSystem = {
    val endpoint = getEndpoint(filePath)
    FileSystem.get(URI.create(endpoint), conf)
  }
  /** Fetches a list of FileStatus objects for this directory, or
   *  if the filePath is a file, just a list containing the file's FileStatus.
   *  @param filePath the file or directory path.
   *  @return Seq[FileStatus]
   */
  def getFileStatusList(filePath: String): Seq[FileStatus] = {
      val conf = configuration
      val fs: FileSystem = getFileSystem(filePath, conf)
      val files = HdfsStore.getFileList(filePath, fs)
      val fileStatusArray = {
        var statusArray = Array[FileStatus]()
        for (file <- files) {
          val fileStatus = fs.getFileStatus(new Path(getFilePath(file)))
          statusArray = statusArray ++ Array(fileStatus)
        }
        statusArray.toSeq
      }
      fileStatusArray
  }
}

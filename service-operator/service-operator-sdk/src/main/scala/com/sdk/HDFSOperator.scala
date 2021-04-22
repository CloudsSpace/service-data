package com.sdk

import com.JDBCOperator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.util.ReflectionUtils


/**
  *
  * @Author: ysh
  * @Date: 2019/10/21 18:55
  * @Version: 1.0
  */
abstract class HDFSOperator extends Operator {

  private var configuration: Configuration = _
  private var fileSystem: FileSystem = _
  private var compressionCodec: CompressionCodec = _

  def initFileSystem(): FileSystem = {
    configuration = new Configuration
    val jDBCOperator = new JDBCOperator
    val nodes = jDBCOperator.getClusterEnv("hdfs")
    configuration.set("fs.defaultFS", nodes)
    configuration.set("dfs.client.use.datanode.hostname", "true")
    configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    try
      fileSystem = FileSystem.get(configuration)
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    jDBCOperator.close()
    fileSystem
  }

  def createCompressionCodec(codec: String): CompressionCodec = {
    compressionCodec = ReflectionUtils.newInstance(Class.forName(codec), configuration).asInstanceOf[CompressionCodec]
    compressionCodec
  }

  def getFileSystem: FileSystem = fileSystem

  def getConfiguration: Configuration = configuration

  def getCompressionCodec: CompressionCodec = compressionCodec

}



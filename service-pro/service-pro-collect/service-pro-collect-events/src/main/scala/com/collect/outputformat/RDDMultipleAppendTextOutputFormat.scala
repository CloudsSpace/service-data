package com.collect.outputformat

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.{JobConf, RecordWriter}
import org.apache.hadoop.mapred.lib.MultipleOutputFormat
import org.apache.hadoop.util.Progressable

/**
  * 根据 key定义输出文件和目录
  * 追加写入数据后 输出路径置位空
  *
  * @Author: ysh
  * @Date: 2019/5/31 16:19
  * @Version: 1.0
  */
class RDDMultipleAppendTextOutputFormat extends MultipleOutputFormat[Any, Any]{
  private var theTextOutputFormat: AppendTextOutputFormat = null

  //产生分区目录
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String ={
    key.toString
  }

  //追加写
  override def getBaseRecordWriter(fs: FileSystem, job: JobConf, name: String, arg3: Progressable): RecordWriter[Any, Any] = {
    if (this.theTextOutputFormat == null) {
      this.theTextOutputFormat = new AppendTextOutputFormat()
    }
    return this.theTextOutputFormat.getRecordWriter(fs, job, name, arg3)
  }

  //key重置为空
  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get()
}

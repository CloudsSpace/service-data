package com.collect.outputformat

import java.io.DataOutputStream

import org.apache.commons.crypto.utils.ReflectionUtils
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.compress.{CompressionCodec, GzipCodec, SnappyCodec}
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf, RecordWriter, TextOutputFormat}
import org.apache.hadoop.util.Progressable

/**
  * 重写HDFS OutPut
  * 追加数据写入HDFS
  *
  * @Author: ysh
  * @Date: 2019/5/31 16:19
  * @Version: 1.0
  */
class AppendTextOutputFormat extends TextOutputFormat[Any, Any] {

  override def getRecordWriter(ignored: FileSystem, job: JobConf, iname: String, progress: Progressable): RecordWriter[Any, Any] = {
    val isCompressed: Boolean = FileOutputFormat.getCompressOutput(job)
    val keyValueSeparator: String = job.get("mapreduce.output.textoutputformat.separator", "\t")
    //自定义输出文件名
    val name = job.get("filename",iname)
    if (!isCompressed) {
      val file: Path = FileOutputFormat.getTaskOutputPath(job, name)
      val fs: FileSystem = file.getFileSystem(job)
      val newFile : Path = new Path(FileOutputFormat.getOutputPath(job), name)
      if (!fs.exists(newFile)) {
        fs.create(file, progress).close()
      }
      val fileOut : FSDataOutputStream = fs.append(newFile)
      new TextOutputFormat.LineRecordWriter[Any, Any](fileOut, keyValueSeparator)
    } else {
      val codecClass: Class[_ <: CompressionCodec] = FileOutputFormat.getOutputCompressorClass(job, classOf[SnappyCodec])
      // create the named codec
      val codec: CompressionCodec = ReflectionUtils.newInstance(codecClass, job)
      // build the filename including the extension
      val file: Path = FileOutputFormat.getTaskOutputPath(job, name + codec.getDefaultExtension)
      val fs: FileSystem = file.getFileSystem(job)
      val newFile : Path = new Path(FileOutputFormat.getOutputPath(job), name + codec.getDefaultExtension)

      if (!fs.exists(newFile)) {
        fs.create(file, progress).close()
      }
      val fileOut: FSDataOutputStream = fs.append(newFile)
      new TextOutputFormat.LineRecordWriter[Any, Any](new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator)
    }
  }

}
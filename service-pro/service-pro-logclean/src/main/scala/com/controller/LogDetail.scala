package com.controller

import com.service.LogDetailService
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @function 埋点数据清洗
  */
object LogDetail {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogDetail")//.setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("error")
    val srcData = sc.textFile("hdfs://datanode:9000/data/ods/cat-end-logconsume.log.2019-09-05")
    LogDetailService.dataClean(srcData).saveAsTextFile("hdfs://datanode:9000/data/dwo/logClean")
    sc.stop()
  }
}

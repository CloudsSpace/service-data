package com.sdk

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  *
  * @Author: ysh
  * @Date: 2019/10/21 18:16
  * @Version: 1.0
  */
trait SparkStreamingOperator extends Operator {

  private var spark: StreamingContext = _

  def initStreamingContext(seconds: Long): StreamingContext = {
    val sparkConf = new SparkConf
    val parameterMap:  mutable.Map[String, String] = getParameterMap
    for ((k, v) <- parameterMap) {
      sparkConf.set(k, v)
    }
    spark = new StreamingContext(sparkConf, Seconds(seconds))
    spark
  }

  def getSparkSession(): StreamingContext = spark
}

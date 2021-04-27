package com.duiba.util


import org.apache.spark.sql.SparkSession
import scala.collection.mutable

trait SparkSqlOperator extends Operator {

  private var spark: SparkSession = _

  def initSparkSession(): SparkSession = {
    val builder: SparkSession.Builder = SparkSession.builder.enableHiveSupport
    val parameterMap: mutable.Map[String, String] = getParameterMap
    for ((k, v) <- parameterMap) {
      builder.config(k, v)
    }
    spark = builder.getOrCreate
    spark
  }

  def getSparkSession(): SparkSession = spark

}

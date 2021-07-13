package com.duiba.task

import com.duiba.exception.JobParamsException
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object ExcelLoad {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private var spark: SparkSession = _
  private var jobParams: JobParams = _
  private val excelView = "exceltable"
  private val warehouseurl = "hdfs://nameservice1/user/hive/warehouse"

  def main(args: Array[String]): Unit = {
    initialize
    jobParams = JobParams(args(0), args(1), args(2))
    val exceldata = readExcel(jobParams.path)
    exceldata.createOrReplaceTempView(excelView)
    customLoad
    close
  }

  def customLoad() = {
    jobParams.mode match {
      case "1" => spark.sql(s"CREATE TABLE IF NOT EXISTS  ${jobParams.tablename} as select * from exceltable")
      case "2" => spark.sql(s"INSERT into TABLE ${jobParams.tablename} select * from exceltable")
      case "3" => spark.sql(s"INSERT overwrite TABLE ${jobParams.tablename} select * from exceltable")
      case _ => throw new JobParamsException(s"params : ${jobParams.mode} ; state : params error ;")
    }
  }

  case class JobParams(path: String, tablename: String, mode: String)

  def excelLoad(file: String) = {
    spark.read.format("com.crealytics.spark.excel")
      .option("header", "true")
      .load(file)
  }

  def readExcel(file: String): DataFrame = spark.read
    .format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "true")
    .option("addColorColumns", "False")
    .load(file)

  def initialize: Unit = {
    spark = SparkSession.builder()
      .appName("ExcelLoad")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", warehouseurl)
      .enableHiveSupport()
      .getOrCreate()
    //    spark.sparkContext.setCheckpointDir("")
    logger.info("initialize success")
  }

  def close: Unit = {
    spark.close()
  }

}

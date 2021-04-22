package com.control.task

import com.JDBCOperator
import com.exception.JobParamsException
import com.parse.util.DwUtil

import java.util
import com.qupeiyin.data.operator.entity.mysqlload.MysqlLoadInfo
import com.qupeiyin.data.operator.sdk.{JDBCOperator, SparkSqlOperator}
import com.qupeiyin.data.operator.sdk.exception.JobParamsException
import com.qupeiyin.data.operator.sdk.job.JobType
import com.qupeiyin.parrot.parse.util.DwUtil
import com.sdk.SparkSqlOperator
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import collection.JavaConverters._
import scala.collection.mutable

/**
 * 数据仓库dwb dwd层数据加载
 */
object MysqlLoad extends SparkSqlOperator {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private var jdbcOperator: JDBCOperator = _
  private var paramMap: util.Map[String, util.HashMap[String, String]] = _
  private var spark: SparkSession = _
  private var dwUtil: DwUtil = _

  def main(args: Array[String]): Unit = {

    val tuple: (Boolean, String) = checkParams(args)
    if (!tuple._1) {
      logger.info("job args error")
      throw new JobParamsException(s"${tuple._2} example :  1,2")
    }
    //初始化参数
    initialize
    //加载需要导入hive的配置信息
    val mysqlLoadInfos: mutable.Seq[MysqlLoadInfo] = dwUtil.getMysqlLoadInfo(if (args.length == 1) args(0) else null).asScala
    //读取mysql表数据写入hive
    mysqlLoadInfos.foreach(mysqlLoadInfo => {
      println(mysqlLoadInfo)
      executeJob(mysqlLoadInfo)
    })

    close
  }

  def executeJob(mysqlLoadInfo: MysqlLoadInfo): Unit = {
    //从mysql读取数据
    var jdbcDF: DataFrame = spark.read
      .format("jdbc")
      .option("url", jdbcOperator.getDbUrl.replace("bidb", mysqlLoadInfo.getSourceDb))
      .option("driver", jdbcOperator.getDriver)
      .option("dbtable", mysqlLoadInfo.getSourceTable)
      .option("user", jdbcOperator.getUsername)
      .option("password", jdbcOperator.getPassword)
      .load()

    //写入数据到hive
    val tableName: String = mysqlLoadInfo.getSinkDb + "." + mysqlLoadInfo.getSinkTable
    val bool: Boolean = spark.catalog.tableExists(tableName)
    if (bool) {
      val strings: Array[String] = spark.catalog.listColumns(tableName).collect().map(_.name)
      jdbcDF = jdbcDF.select(strings.head, strings.tail: _*)
    }
    jdbcDF.write.mode(SaveMode.Overwrite).format("orc").saveAsTable(tableName)
  }


  override def initialize: Unit = {
    jdbcOperator = new JDBCOperator()
    paramMap = jdbcOperator.getAppParams(this.getClass)
    setParameterMap(paramMap.get(JobType.SPARK.getType).asScala)
    spark = initSparkSession()
    dwUtil = new DwUtil
    logger.info("initialize success")
  }

  override def close: Unit = {
    spark.close()
    dwUtil.close()
  }

  def checkParams(args: Array[String]): (Boolean, String) = {
    if (args.length == 0) {
      (true, null)
    } else if (args.length != 1) {
      (false, "params error")
    } else {
      val ids: Array[String] = args(0).split(",")
      ids.foreach(id => {
        val bool: Boolean = StringUtils.isNumeric(id)
        if (!bool) {
          return (false, "params error")
        }
      })
      (true, null)
    }
  }


}

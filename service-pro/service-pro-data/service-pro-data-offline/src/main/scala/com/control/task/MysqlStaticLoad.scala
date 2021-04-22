package com.control.task

import com.entity.mysqlstatic.{MysqlSyncStatic, MysqlSyncStaticFields}
import com.exception.JobParamsException
import com.parse.util.HDFSEventOperator

import java.sql.Connection
import com.qupeiyin.data.operator.entity.mysqlstatic.{MysqlSyncStatic, MysqlSyncStaticFields}
import com.qupeiyin.data.operator.sdk.Operator
import com.qupeiyin.data.operator.sdk.exception.JobParamsException
import com.qupeiyin.data.operator.sdk.util.date.{DateStyle, DateUtil}
import com.qupeiyin.data.operator.sdk.util.{C3p0Util, HiveUtil}
import com.qupeiyin.parrot.parse.util.HDFSEventOperator
import com.sdk.Operator
import com.util.date.{DateStyle, DateUtil}
import com.util.{C3p0Util, HiveUtil}
import groovy.lang.Tuple
import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}

import collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @Classname MysqlStaticLoad
  * @Description TODO
  * @Date 2020/2/20 14:49
  * @Created by ysh
  */
object MysqlStaticLoad extends Operator {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private var staticDate: String = DateUtil.getYesterday(DateStyle.YYYY_MM_DD)
  private val dataPath: String = "/data/binlog"
  private val mysqlSyncOffline = "bidb.mysql_sync_static"
  private val mysqlSyncOfflineFields = "bidb.mysql_sync_static_fields"

  private var hiveUtil: HiveUtil = _
  private var c3p0Util: C3p0Util = _
  private var fSEventOperator : HDFSEventOperator = _
  private var conn: Connection = _

  def main(args: Array[String]): Unit = {
    initialize

    checkParams(args)
    execute(getTable)

    close
  }


  def execute(mysqlSyncStatics: Map[String, mutable.Buffer[MysqlSyncStaticFields]]) {
    hiveUtil.setSparkAppName(this.getClass.getName)
    mysqlSyncStatics.foreach(mysqlSyncStatic => {

      val hiveTable = mysqlSyncStatic._1
      val logPath = s"$dataPath/$hiveTable/$staticDate"

      if(fSEventOperator.exists(logPath)){
        //建表
        val ddl = odsDdl(hiveTable, mysqlSyncStatic._2)
        logger.info(s"create:$ddl")
        hiveUtil.execute(ddl)

        //加载数据
        val load = s"load data  inpath '$logPath' into table ods.$hiveTable partition(staticdate='$staticDate')"
        logger.info(s"load:$load")
        hiveUtil.execute(load)
      }else{
        logger.warn(s"path:$logPath is not exists or any files")
      }

    })
  }

  def odsDdl(table: String, fields: mutable.Buffer[MysqlSyncStaticFields]): String = {
    val builder = new mutable.StringBuilder("create external table if not exists ods.")
    builder.append(table + "(")
    val buffer = new ListBuffer[String]
    fields.foreach(tuple => {
      val fType = tuple.getType match {
        case 1 => "String"
        case 2 => "Bigint"
        case 3 => "Boolean"
        case 4 => "Double"
        case _ => "String"
      }
      buffer.+=(tuple.getName + " " + fType)
    })
    builder.append(StringUtils.join(buffer.asJava, ","))
    builder.append(") partitioned by (staticdate string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS TEXTFILE")
    builder.toString()
  }

  def checkParams(args: Array[String]): Unit = {
    logger.info("check params")
    if (args.length == 1) {
      if (DateUtil.isDateString(args(0))) {
        staticDate = args(0)
      } else {
        throw new JobParamsException("date style is error , example yyyy-MM-dd")
      }
    }
  }




  def getTable: Map[String, mutable.Buffer[MysqlSyncStaticFields]]= {
    val ps1 = conn.prepareStatement("select * from " + mysqlSyncOffline)
    val ps2 = conn.prepareStatement("select * from " + mysqlSyncOfflineFields)
    val mysqlSyncOfflineSet = ps1.executeQuery()
    val mysqlSyncOfflineFieldsSet = ps2.executeQuery()

    val mysqlSyncStatics = C3p0Util.putResult(mysqlSyncOfflineSet, classOf[MysqlSyncStatic]).asScala
    val mysqlSyncStaticFields = C3p0Util.putResult(mysqlSyncOfflineFieldsSet, classOf[MysqlSyncStaticFields]).asScala

    val fieldsMap = mysqlSyncStaticFields.groupBy(entity => entity.getFieldId).map(entity => {
      (entity._1, entity._2.sortBy(_.getId))
    })

    val map: Map[String, mutable.Buffer[MysqlSyncStaticFields]] = mysqlSyncStatics.map(mysqlSyncStatic => {
      val hiveTable = mysqlSyncStatic.getHiveTable
      (hiveTable, fieldsMap(mysqlSyncStatic.getFieldId))
    }).toMap

    ps1.close()
    ps2.close()

    map
  }


  override def initialize: Unit = {
    System.setProperty("HADOOP_USER_NAME", "hive")
    hiveUtil = new HiveUtil
    c3p0Util = new C3p0Util
    fSEventOperator = new HDFSEventOperator
    conn = c3p0Util.getConnection
    logger.info("initialize success")
  }

  override def close: Unit = {
    hiveUtil.close()
    c3p0Util.close()
    fSEventOperator.close()
    logger.info("closed")
  }


}


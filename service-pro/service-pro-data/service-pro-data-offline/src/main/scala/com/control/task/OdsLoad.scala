package com.control.task

import java.util.concurrent.{CountDownLatch, ExecutorService, Executors}
import java.util.Date

import com.alibaba.fastjson
import com.alibaba.fastjson.{JSON, JSONObject}
import com.qupeiyin.data.operator.entity.warehouse.EventInfo
import com.qupeiyin.data.operator.sdk.job.JobType
import com.qupeiyin.data.operator.sdk.util.date.DateUtil
import com.qupeiyin.data.operator.sdk.{JDBCOperator, SparkSqlOperator}
import com.qupeiyin.parrot.control.util.HiveUtils
import com.qupeiyin.parrot.parse.filter.{HdfsDelayFileFilter, HdfsFileFilter}
import com.qupeiyin.parrot.parse.util.{DwUtil, HDFSEventOperator}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.collection.{JavaConversions, JavaConverters, mutable}

/**
  * 对落地在HDFS的用户行为数据进行处理入库
  * 对每一个事件形成一个Job并行写入Hive ods
  *
  * @Author: ysh
  * @Date: 2019/5/31 16:19
  * @Version: 1.0
  */
class OdsLoad extends Serializable {

  private val logger = OdsLoad.logger
  //private val structTypes = Array("LongType", "StringType", "BooleanType", "DoubleType")

  /**
    * 根据原始数据dataFrame和schema转换成新的dataFrame
    *
    * @param schema        事件原始schema
    * @param originalFrame 事件数据集
    * @param spark         spark
    * @return scala.Tuple2<org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>,org.apache.spark.sql.types.StructField[]>
    */
  def parse(schema: JSONObject, originalFrame: DataFrame, spark: SparkSession): (DataFrame, Array[StructField]) = {

    logger.info(s"parse schema : $schema")
    val fields: java.util.Set[String] = schema.keySet()
    val filedAndType = new mutable.LinkedHashMap[String, DataType]
    val cols = new ListBuffer[String]
    for (field <- JavaConverters.asScalaSetConverter(fields).asScala) {
      val col: String = field.split("=")(1).toLowerCase
      if (!cols.contains(col)) {
        val dataType: Int = schema.get(field).asInstanceOf[Int]
        dataType match {
          case 1 => filedAndType.put(field, StringType)
          case 2 => filedAndType.put(field, LongType)
          case 3 => filedAndType.put(field, BooleanType)
          case 4 => filedAndType.put(field, DoubleType)
          case _ => filedAndType.put(field, StringType)
        }
        cols.+=(col)
      }
    }

    logger.info("parse json data")
    val result = originalFrame.toJSON.rdd.map(json => {
      var row = Row()
      val jsonObj = JSON.parseObject(json)
      val proObj = jsonObj.getJSONObject("properties")
      for ((fields, dataType) <- filedAndType) {
        val fieldArr: Array[String] = fields.split("=")
        fieldArr(0) match {
          //事件初始字段 project event ...
          case "event" => row = Row.merge(row, getRow(dataType, jsonObj, fieldArr(1)))
          //$ : 神策SDK自带字段 , 其余为手动埋点字段
          case "properties" => row = Row.merge(row, getRow(dataType, proObj, fieldArr(1)))
          //预处理字段
          //case "pre" => row = Row.merge(row, Row(DateUtil.getDateTimeMilli(new Date(jsonObj.getLongValue("time")))))
          case "pre" => row = Row.merge(row, Row(null))
          case _ =>
        }
      }
      row
    })

    logger.info("create dataFrame")
    //创建新dataframe
    val structField: Array[StructField] = filedAndType.toArray.map(fieldType => StructField(fieldType._1.split("=")(1).replace("$", "e_"), fieldType._2, nullable = true))
    val resultFrame: DataFrame = spark.createDataFrame(result, new StructType(structField))
    (resultFrame, structField)
  }


  val getRow = (dataType: DataType, jsonObj: fastjson.JSONObject, field: String) => {
    var row = Row()
    try {
      dataType match {
        case LongType => row = Row(jsonObj.getLong(field))
        case DoubleType => row = Row(jsonObj.getDouble(field))
        case StringType => row = Row(jsonObj.getString(field))
        case BooleanType => row = Row(jsonObj.getBoolean(field))
        case _ => row = Row(jsonObj.getString(field))
      }
    } catch {
      case _: Exception => row = Row(null)
    }
    row
  }


  /*def createSchema(fileName: String, structType: StructType): (String, String) = {
    val filedObj = new JSONObject(true)
    for (structField <- structType) {
      val fieldType: String = structField.dataType.toString
      val fieldName: String = structField.name

      if (fieldName.equals("properties")) {
        val properties: StructType = structField.dataType.asInstanceOf[StructType]
        for (pro <- properties) {
          val proFiledType: String = pro.dataType.toString
          val proFiledName: String = pro.name
          if (structTypes.contains(proFiledType)) {
            filedObj.put(s"properties&$proFiledName", proFiledType)
          } else {
            filedObj.put(s"properties&$proFiledName", "StringType")
          }
        }

      } else {
        if (structTypes.contains(fieldType)) {
          filedObj.put(s"event&$fieldName", fieldType)
        } else {
          filedObj.put(s"event&$fieldName", "StringType")
        }
      }
    }

    //预处理字段
    //filedObj.put("pre&event_time", "StringType")
    (fileName, filedObj.toJSONString)
  }*/

}


object OdsLoad extends SparkSqlOperator {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private var jdbcOperator: JDBCOperator = _
  var jobSubmitExecutors: ExecutorService = _
  var spark: SparkSession = _
  var staticDate: String = _
  var writeMode: String = _
  var filterFilter: String = _
  private val executors = 5
  private var hdfsEventOperator: HDFSEventOperator = _
  private var dwUtil: DwUtil = _

  /**
    * 读取每日原始数据json格式 project+event
    * 对原始数据进行转化后写入hive分区表中
    */
  def main(args: Array[String]): Unit = {

    if (args.length == 2) {
      staticDate = args(0)
      args(1) match {
        case "1" =>
          writeMode = "insert overwrite"
          filterFilter = classOf[HdfsFileFilter].getCanonicalName
        case "-1" =>
          filterFilter = classOf[HdfsDelayFileFilter].getCanonicalName
          writeMode = "insert into"
        case _ => return
      }
    } else {
      return
    }

    //构建环境
    initialize
    val importData = new OdsLoad
    spark.sparkContext.hadoopConfiguration.set("mapreduce.input.pathFilter.class", filterFilter)
    //获取所有事件文件路径和表名
    logger.info("load event path")
    val tablePathList: mutable.LinkedHashMap[String, String] = hdfsEventOperator.getEventPathList(staticDate)
    logger.info("load event metadata")
    val eventMap: java.util.HashMap[String, EventInfo] = dwUtil.getEventInfoMap
    val latch: CountDownLatch = new CountDownLatch(tablePathList.size)

    //多线程提交sparkJob
    for ((path, fileName) <- tablePathList) {
      jobSubmitExecutors.execute(new Runnable {
        override def run(): Unit = {
          try {
            val start = System.currentTimeMillis()
            logger.info(s"run event:$fileName path:$path")
            //var eventInfo: EventInfo = eventMap.get(fileName)
            //var frame: DataFrame = null
            //判断是否是新增事件
            /*if (eventInfo == null){
              logger.info(s"new add event:$fileName")
              frame = spark.read.json(path)
              val tuple: (String, String) = importData.createSchema(fileName, frame.schema)
              eventInfo = dwUtil.createEventInfo(tuple._1, tuple._2)
              //判断事件是否入库
            } else if (eventInfo.getStatus == 0) {
              logger.info(s"invalid event:$fileName")
              return
            } else {
              frame = spark.read.json(path)
            }*/

            val eventInfo = eventMap.get(fileName)
            if (eventInfo == null) return
            val frame = spark.read.json(path)
            val tableName: String = eventInfo.getTableName
            //解析字段转换dataFrame
            logger.info(s"parse json data event:$fileName")
            val tuple: (DataFrame, Array[StructField]) = importData.parse(dwUtil.convertFields(eventInfo.getFields), frame, spark)

            //注册临时表
            logger.info(s"create dataFrame")
            tuple._1.coalesce(10).createOrReplaceTempView(tableName + "_tmp")
            val create: String = HiveUtils.createTable(tableName, tuple._2)

            //建表
            logger.info(s"create hive table sql:$create")
            spark.sql(create)
            //写入数据
            val insert = s"$writeMode table ods.$tableName partition(staticdate='$staticDate') select * from ${tableName}_tmp"
            spark.sql(insert)
            logger.info(s"save data sql:$insert")
            val end = System.currentTimeMillis()
            logger.info(s"exec event:$fileName  total_time:${(end-start)/1000d}s")
          } catch {
            case e: Exception =>
              logger.error(s"event load error $fileName = $path msg:${e.getMessage}")
          } finally {
            latch.synchronized {
              latch.countDown()
            }
          }
        }
      })
    }
    //释放资源
    latch.await()
    close
  }

  import collection.JavaConverters._

  override def initialize {
    jdbcOperator = new JDBCOperator()
    val paramMap = jdbcOperator.getAppParams(this.getClass)
    setParameterMap(paramMap.get(JobType.SPARK.getType).asScala)
    spark = initSparkSession()
    jobSubmitExecutors = Executors.newFixedThreadPool(executors)
    hdfsEventOperator = new HDFSEventOperator
    dwUtil = new DwUtil
    logger.info("initialize success")
  }

  override def close: Unit = {
    hdfsEventOperator.close()
    spark.stop()
    dwUtil.close()
    jdbcOperator.close()
    jobSubmitExecutors.shutdown()
    logger.info("closed")
  }
}

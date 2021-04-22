package com.control.task

import com.JDBCOperator

import java.{lang, util}
import java.util.concurrent.{CountDownLatch, ExecutorService, Executors}
import com.alibaba.fastjson.JSON
import com.entity.warehouse.DwEntity
import com.exception.JobParamsException
import com.job.JobType
import com.parse.util.DwUtil
import com.sdk.SparkSqlOperator
import com.util.date.DateUtil
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import collection.JavaConverters._
import scala.collection.{JavaConversions, mutable}

/**
  * 数据仓库dwb dwd层数据加载
  */
object DwLoad extends SparkSqlOperator {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private var jdbcOperator: JDBCOperator = _
  private var paramMap: util.Map[String, util.HashMap[String, String]] = _
  private var jobSubmitExecutors: ExecutorService = _
  private val executors = 3
  private var spark: SparkSession = _
  private var dwUtil: DwUtil = _
  private var jobParams: JobParams = _
  private val dws = Set("dwd", "dws", "mid", "dim","ads")

  def main(args: Array[String]): Unit = {

    val tuple = checkParams(args)
    if (!tuple._1) {
      logger.info("job args error")
      throw new JobParamsException(s"${tuple._2} example : dwd 2019-01-01 ")
    }
    //初始化参数
    jobParams = JobParams(args(0), DateUtil.addDay(args(1), 1))
    initialize
    val dw = dwUtil.getDwInfo(jobParams.dw).asScala
    val sortDws = dw.toList.sortBy(_._1)

    sortDws.foreach(dwsTuple => {
      executeJobList(dwsTuple._2.asScala)
    })

    close
  }

  def staticdateReplace(sql: String): String = {
    sql.replace("current_date()", s"'${jobParams.staticdate}'").
      replace("CURRENT_DATE()", s"'${jobParams.staticdate}'").
      replace("current_date", s"'${jobParams.staticdate}'").
      replace("CURRENT_DATE", s"'${jobParams.staticdate}'")
  }

  def executeJobList(jobList: mutable.Buffer[DwEntity]): Unit = {
    val latch: CountDownLatch = new CountDownLatch(jobList.size)
    jobList.foreach(job => {
      jobSubmitExecutors.execute(new Runnable {

       /* def writeHbase(sql: String) {
          val frame = spark.sql(sql)
          if (!frame.isEmpty) {
            val hbaseJob = createHbaseJob(job.getExternalName)
            val fields = frame.schema.fields.map(x => Bytes.toBytes(x.name))
            frame.rdd.map(row => {
              val put = new Put(Bytes.toBytes(row.get(0).toString))
              for (index <- 1 until fields.length - 1) {
                val value = row.get(index)
                if (value != null) {
                  put.addColumn(Bytes.toBytes("cf"), fields(index), Bytes.toBytes(value.toString))
                }
              }
              put
            }).filter(!_.isEmpty).map(put => (new ImmutableBytesWritable(), put)).saveAsNewAPIHadoopDataset(hbaseJob.getConfiguration)
          }
        }*/

        override def run(): Unit = {
          logger.info(s"start... <=======================${job.getSink}=======================>")
          try {
            //加载当前表
            val execSqls = job.getExecSqls
            if (execSqls != null && !"".equals(execSqls)) {
              val sqls: mutable.Buffer[String] = JSON.parseArray(execSqls, classOf[String]).asScala
              sqls.foreach(sqlStr => {
                val start = System.currentTimeMillis()
                val sql = staticdateReplace(sqlStr)
                logger.info(s"exec sql:$sql")
                job.getExternal match {
                  //hive表
                  case 0 => spark.sql(sql)
                  //hbase外表
                  case _ => logger.warn("暂不支持其他外部表")
                }
                val end = System.currentTimeMillis()
                logger.info(s"exec sql:$sql  total_time:${(end-start) / 1000d}s")
              })
            }
            logger.info(s"finish... <=======================${job.getSink}=======================>")
          } catch {
            case e: Exception => e.printStackTrace()
              logger.error(s"exec error ${job.getSink} msg:${e.getMessage}")
          } finally {
            latch.synchronized {
              latch.countDown()
            }
          }
        }
      })
    })
    latch.await()

  }

  override def initialize: Unit = {
    jdbcOperator = new JDBCOperator()
    paramMap = jdbcOperator.getAppParams(this.getClass)
    setParameterMap(paramMap.get(JobType.SPARK.getType).asScala)
    jobSubmitExecutors = Executors.newFixedThreadPool(executors)
    spark = initSparkSession()
    dwUtil = new DwUtil
    logger.info("initialize success")
  }

  override def close: Unit = {
    spark.close()
    dwUtil.close()
  }

  case class JobParams(dw: String, staticdate: String)

  def checkParams(args: Array[String]): (Boolean, String) = {
    if (args.length != 2) {
      return (false, "params is error")
    } else if (!dws.contains(args(0))) {
      return (false, s"${args(0)} dw is not exists")
    } else if (!DateUtil.isDate(args(1))) {
      return (false, s"${args(1)} data style is error")
    }
    (true, "")
  }

  def createHbaseJob(hTable: String): Job = {
    val configuration = spark.sparkContext.hadoopConfiguration
    val hbaseMap = JavaConversions.mapAsScalaMap(paramMap.get(JobType.HBASE.getType))
    hbaseMap.foreach(param => configuration.set(param._1, param._2))
    configuration.set("hbase.mapred.outputtable", hTable)

    val hbaseBulkJob = Job.getInstance(configuration)
    hbaseBulkJob.setOutputKeyClass(classOf[ImmutableBytesWritable])
    hbaseBulkJob.setOutputValueClass(classOf[Result])
    hbaseBulkJob.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    hbaseBulkJob
  }

}

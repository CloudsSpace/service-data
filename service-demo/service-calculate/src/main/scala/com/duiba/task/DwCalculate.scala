package com.duiba.task

import com.duiba.util.{DwUtil, JDBCOperator, SparkSqlOperator}

import java.io.IOException
import java.util.concurrent.{CountDownLatch, ExecutorService, Executors, TimeUnit}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

object DwCalculate extends SparkSqlOperator {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private var jdbcOperator: JDBCOperator = _
  private var paramMap: java.util.Map[String, java.util.HashMap[String, String]] = _
  private var jobSubmitExecutors: ExecutorService = _
  private var spark: SparkSession = _
  private var jobParams: JobParams = _
  private val dws = Set("dwd", "dws", "mid", "dim", "ads")
  private val ckPath: String = "hdfs://bigdata-2:8020/data/datacombine"
  private var configuration: Configuration = _
  private var fileSystem: FileSystem = _
  private var hdfsUrl = "hdfs://bigdata-2:8020"
  private var combineUrl: String = _
  private var DwUtil: DwUtil = _
  private val executors = 3
  private val partitionSize = 100

  def main(args: Array[String]): Unit = {

    jobParams = JobParams(if (args.length >= 1) args(0) else null, if (args.length >= 2) args(1) else null, if (args.length == 3) args(2) else null)
    initialize
    val dw = DwUtil.getInfo("1")
    println(dw)

//    val dw = DwUtil.getInfo(jobParams.dw, if (parms(jobParams) == "DirCombine") jobParams.cumstom else jobParams.jobId).asScala

//    dw.foreach(job => {
//
//
//    })
    close
    logger.info("==================combine stop====================")
  }

//
//  /**
//    * url拼接
//    *
//    * @param spark
//    * @param source
//    * @param sourceType
//    * @return
//    */
//  def customUrl(spark: SparkSession, srcTable: String) = {
//
//    //1.根据表名获取表的url地址
//    val db = new Regex(".*(?=\\.)").findFirstIn(srcTable).getOrElse("other")
//    val table = new Regex("(?<=\\.).*").findFirstIn(srcTable).getOrElse("other")
//    logger.info(s"db: ${db}   table:${table}")
//
//    val url = spark.catalog.getDatabase(db).locationUri
//    val combineUrl = url + "/" + table
//    logger.info(s"combineUrl: ${combineUrl}")
//    //2.获取表下所有目录的全路径
//    val arr = ArrayBuffer[String]()
//    val dirPaths = fileSystem.listStatus(new Path(combineUrl)).flatMap(dirPath => {
//      listPath(dirPath, arr)
//      arr
//    }).distinct.filter(path => fileSystem.listStatus(new Path(path.toString)).length > 0)
//    dirPaths
//  }
//
//  /**
//    * 自定义文件合并（根据时间）
//    *
//    * @param combinePath
//    * @return
//    */
//  def customCombine(execTable: String, combinePath: Array[String]) = {
//    val latch: CountDownLatch = new CountDownLatch(combinePath.size)
//    logger.info("=====================CustomCombine In Progress=====================")
//
//    combinePath.foreach(line => {
//      jobSubmitExecutors.execute(new Runnable {
//        override def run(): Unit = {
//          try {
//            if (line.contains(s"staticdate=${jobParams.cumstom}")) {
//              logger.info("File Combine In Progress")
//
//              val datafromat = storageFormat(spark, execTable)
//              val partitionNum = customPartitions(fileSystem, line, partitionSize)
//              logger.info("Combine Path: " + line + "   " + "Number of Partitions: " + partitionNum + "Storage Format:" + datafromat)
//              spark.read.format(datafromat).load(line).checkpoint().coalesce(partitionNum).write.mode(SaveMode.Overwrite).format(datafromat)
//                .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
//                .save(line)
//            }
//          } catch {
//            case e: Exception => e.printStackTrace()
//              logger.error(s"exec error ${line} msg:${e.getMessage}")
//          } finally {
//            latch.synchronized {
//              latch.countDown()
//            }
//          }
//        }
//      })
//    })
//    latch.await()
//  }

  case class JobParams(dw: String, cumstom: String, jobId: String)

  override def initialize: Unit = {
    jdbcOperator = new JDBCOperator()
    //    paramMap = jdbcOperator.getAppParams(this.getClass)
    jobSubmitExecutors = Executors.newFixedThreadPool(executors)
    spark = initSparkSession()
    spark.sparkContext.setCheckpointDir(ckPath)
    DwUtil = new DwUtil
    logger.info("initialize success")
  }

  override def close: Unit = {
    spark.close()
  }

}

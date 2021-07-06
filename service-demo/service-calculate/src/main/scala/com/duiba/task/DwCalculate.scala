package com.duiba.task

import com.duiba.util.{DwUtil, JDBCOperator, SparkSqlOperator}

import java.util.concurrent.{CountDownLatch, ExecutorService, Executors, TimeUnit}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters.asScalaBufferConverter


object DwCalculate extends SparkSqlOperator {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private var jdbcOperator: JDBCOperator = _
  private var paramMap: java.util.Map[String, java.util.HashMap[String, String]] = _
  private var jobSubmitExecutors: ExecutorService = _
  private var spark: SparkSession = _
  private var jobParams: JobParams = _
  private var DwUtil: DwUtil = _
  private val executors = 5

  def main(args: Array[String]): Unit = {

//    jobParams = JobParams(if (args.length >= 1) args(0) else null, if (args.length >= 2) args(1) else null)
//    initialize
//    val dw = DwUtil.getInfo(jobParams.jobId).asScala
//    val jobs = dw.map(line => line.getScript.replaceAll("\\$\\{date\\}", jobParams.jobDate)).toArray
//    customCalculate(jobs)
//    close

//    initialize
//    val dw = DwUtil.getInfo("").asScala
//    val jobs = dw.map(line => line.getScript.replaceAll("\\$\\{date\\}", "2021-04-25")).toArray
//    customCalculate(jobs)
//    for (job<-jobs){
//      println(job)
//    }

  }

  /**
    * 自定义执行
    *
    * @param job
    */
  def customCalculate(job: Array[String]) = {
    val latch: CountDownLatch = new CountDownLatch(job.size)

    logger.info(s"exec job number ${job.size}")

    job.foreach(line => {
      jobSubmitExecutors.execute(new Runnable {
        override def run(): Unit = {
          try {
            logger.info(s"exec sql: ${line}")
            //            spark.sql(line)
          } catch {
            case e: Exception => e.printStackTrace()
              logger.error(s"exec error ${line} msg:${e.getMessage}")
          } finally {
            latch.synchronized {
              latch.countDown()
            }
          }
        }
      })
    })

    latch.await()

    latch.getCount match {
      case 0 => jobSubmitExecutors.shutdown()
      case _ => latch.await()
    }
  }

  case class JobParams(jobDate: String, jobId: String)

  override def initialize: Unit = {
    jdbcOperator = new JDBCOperator()
    jobSubmitExecutors = Executors.newFixedThreadPool(executors)
    spark = SparkSession.builder()
      .appName("DwCalculate")
      .master("local[*]")
      .config("hive.metastore.uris", "thrift://10.50.10.16:9083")
      .config("spark.sql.warehouse.dir", "hdfs://10.50.10.16:9000/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()
    DwUtil = new DwUtil
    logger.info("initialize success")
  }

  override def close: Unit = {
    jdbcOperator.close()
    spark.close()
  }

}

package com.task

import com.JDBCOperator

import java.io.IOException
import java.util.concurrent.{CountDownLatch, ExecutorService, Executors, TimeUnit}
import scala.collection.JavaConverters._
import com.sdk.SparkSqlOperator
import com.util.CombineUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.log4j.lf5.LogLevel
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

object DataCombine extends SparkSqlOperator {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private var jdbcOperator: JDBCOperator = _
  private var paramMap: java.util.Map[String, java.util.HashMap[String, String]] = _
  private var jobSubmitExecutors: ExecutorService = _
  private var logCollect: LogCollect = _
  private var spark: SparkSession = _
  private var jobParams: JobParams = _
  private val dws = Set("dwd", "dws", "mid", "dim", "ads")
  private val ckPath: String = "hdfs://bigdata-2:8020/data/datacombine"
  private var configuration: Configuration = _
  private var fileSystem: FileSystem = _
  private var hdfsUrl = "hdfs://bigdata-2:8020"
  private var combineUrl: String = _
  private var combineUtil: CombineUtil = _
  private val executors = 3
  private val partitionSize = 100

  def main(args: Array[String]): Unit = {

    jobParams = JobParams(if (args.length >= 1) args(0) else null, if (args.length >= 2) args(1) else null, if (args.length == 3) args(2) else null)
    initialize
    initFileSystem
    logger.info("==================combine start====================")
    val dw = combineUtil.getInfo(jobParams.dw, if (parms(jobParams) == "DirCombine") jobParams.cumstom else jobParams.jobId).asScala
    dw.foreach(job => {
      logger.info("exec job:" + job.getSink)

      val lastSize = statistics(spark, job.getSink)
      val lastBlocks = CombineUtil.getHDFSBlocks(fileSystem, spark, job.getSink)

      val combinePath = customUrl(spark, job.getSink)

      if (parms(jobParams) == "CustomCombine") {
        customCombine(job.getSink, combinePath)
      } else {
        dirCombine(job.getSink, combinePath)
      }

      spark.catalog.refreshTable(job.getSink)

      val currentSize = statistics(spark, job.getSink)
      val currentBlocks = CombineUtil.getHDFSBlocks(fileSystem, spark, job.getSink)
      val blocksRatio = (lastBlocks - currentBlocks) * 1.0 / lastBlocks * 1.0

      logger.info(s"Combine Table Name: ${job.getSink} 合并前总数: ${lastSize}  合并后总数: ${currentSize}  合并前数据块个数:${lastBlocks}  合并后数据块个数:${currentBlocks}  数据块降低百分比:${blocksRatio}")

      logCollect.collectJobLog(logger, LogLevel.ERROR, 10, s"" +
        s"【表名】: ${job.getSink} " +
        s"【合并前总数】：${lastSize} 【合并后总数】：${currentSize}" +
        s"【合并前数据块个数】：${lastBlocks}  【合并后数据块个数】：${currentBlocks}" +
        s"【数据块降低占比】：${blocksRatio}", DataTopic.JOB_LOG.getTopic)

    })
    close
    logger.info("==================combine stop====================")
  }


  /**
    * url拼接
    *
    * @param spark
    * @param source
    * @param sourceType
    * @return
    */
  def customUrl(spark: SparkSession, srcTable: String) = {

    //1.根据表名获取表的url地址
    val db = new Regex(".*(?=\\.)").findFirstIn(srcTable).getOrElse("other")
    val table = new Regex("(?<=\\.).*").findFirstIn(srcTable).getOrElse("other")
    logger.info(s"db: ${db}   table:${table}")

    val url = spark.catalog.getDatabase(db).locationUri
    val combineUrl = url + "/" + table
    logger.info(s"combineUrl: ${combineUrl}")
    //2.获取表下所有目录的全路径
    val arr = ArrayBuffer[String]()
    val dirPaths = fileSystem.listStatus(new Path(combineUrl)).flatMap(dirPath => {
      listPath(dirPath, arr)
      arr
    }).distinct.filter(path => fileSystem.listStatus(new Path(path.toString)).length > 0)
    dirPaths
  }

  /**
    * 目录合并
    *
    * @param combinePath
    * @param partitionName
    */
  def dirCombine(execTable: String, combinePath: Array[String]) = {
    logger.info("=====================CombineDir In Progress=====================")
    //1.过滤合并过的目录
    val dirs = combinePath.filter(path => {
      logger.info("Orc or Parquet File Filter In Progress")
      val fileUrl = fileSystem.listStatus(new Path(path.toString)).map(x => x.getPath.toString).apply(0)
      fileUrl match {
        case _ if fileUrl.contains("snappy.orc") => false
        case _ if fileUrl.contains("snappy.parquet") => false
        case _ => true
      }
    })

    val latch: CountDownLatch = new CountDownLatch(dirs.size)
    dirs.foreach(line => {
      logger.info("File Combine In Progress")

      jobSubmitExecutors.execute(new Runnable {
        override def run(): Unit = {
          try {
            val datafromat = storageFormat(spark, execTable)
            val partitionNum = customPartitions(fileSystem, line, partitionSize)
            logger.info("Combine Path: " + line + "   " + "Number of Partitions: " + partitionNum + "Storage Format:" + datafromat)
            spark.read.format(datafromat).load(line).checkpoint().coalesce(partitionNum).write.mode(SaveMode.Overwrite).format(datafromat)
              .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
              .save(line)
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
  }

  /**
    * 自定义文件合并（根据时间）
    *
    * @param combinePath
    * @return
    */
  def customCombine(execTable: String, combinePath: Array[String]) = {
    val latch: CountDownLatch = new CountDownLatch(combinePath.size)
    logger.info("=====================CustomCombine In Progress=====================")

    combinePath.foreach(line => {
      jobSubmitExecutors.execute(new Runnable {
        override def run(): Unit = {
          try {
            if (line.contains(s"staticdate=${jobParams.cumstom}")) {
              logger.info("File Combine In Progress")

              val datafromat = storageFormat(spark, execTable)
              val partitionNum = customPartitions(fileSystem, line, partitionSize)
              logger.info("Combine Path: " + line + "   " + "Number of Partitions: " + partitionNum + "Storage Format:" + datafromat)
              spark.read.format(datafromat).load(line).checkpoint().coalesce(partitionNum).write.mode(SaveMode.Overwrite).format(datafromat)
                .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
                .save(line)
            }
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
  }

  /**
    * 表合并
    *
    * @param combinePath
    * @param partitionName
    */
  def combineTable(combinePath: String, partitionName: String, sourceType: Int) = {
    val dirs = fileSystem.listStatus(new Path(combinePath)).filter(status => status.isDirectory).size
    if (sourceType == 1 && !partitionName.isEmpty && dirs != 0) {
      //分区表全量合并
      logger.info("exec type：Partition table full update ")
      logger.info("combine path：" + combinePath)

      val partitionNum = customPartitions(fileSystem, combinePath, partitionSize)
      spark.read.orc(combinePath).checkpoint().coalesce(partitionNum).write.mode(SaveMode.Overwrite).format("orc").partitionBy(partitionName).save(combinePath)

      logger.info("partition num：" + partitionNum)
      logger.info("data combine success")
    }
  }

  /**
    * 根据文件大小自定义分区数
    *
    * @param fs
    * @param combinePath
    * @param partitionSize
    * @return
    */
  def customPartitions(fs: FileSystem, combinePath: String, partitionSize: Int) = {
    val path = new Path(combinePath)
    logger.info("customPartitions combinePath:" + combinePath)
    try {
      val filesize = fs.getContentSummary(path).getLength
      logger.info("filesize:" + filesize)
      val dirsize = fs.listStatus(new Path(combinePath)).filter(status => status.isDirectory).size match {
        case 0 => 1
        case _ => fs.listStatus(new Path(combinePath)).filter(status => status.isDirectory).size
      }
      val msize = filesize.asInstanceOf[Double] / 1024 / 1024 / dirsize / partitionSize
      logger.info("msize:" + msize)
      Math.ceil(msize).toInt
    } catch {
      case e: IOException => e.printStackTrace()
        1
    }
  }

  /**
    * 文件夹路径遍历
    *
    * @param fs
    * @param array
    */
  def listPath(fs: FileStatus, array: ArrayBuffer[String]): Unit = {
    val path = fs.getPath
    if (fs.isDirectory) {
      val sta = fileSystem.listStatus(new Path(path.toString)).filter(s => s.isDirectory)
      if (sta.length > 0) {
        sta.foreach(x => listPath(x, array))
      } else if (sta.length == 0) {
        array.append(path.toString)
      }
    }
  }

  /**
    * 根据文件格式调用spark不同的api读取数据
    *
    * @param spark
    * @param src
    * @return
    */
  def dataFormat(spark: SparkSession, src: String) = {
    spark.sparkContext.textFile(src)
      .filter(lines => lines.contains("ORC") || lines.contains("parquet")).map(line => {
      line match {
        case _ if line.contains("ORC") == true => "orc"
        case _ => "parquet"
      }
    }).distinct().collect().apply(0)
  }

  /**
    * 文件格式判断（Orc,Parquet,Text）
    *
    * @param spark
    * @param execTable
    * @return
    */
  def storageFormat(spark: SparkSession, execTable: String) = {
    val statement = spark.sql(s"show create table ${execTable}").collect().mkString("\t")
    statement match {
      case _ if statement.contains("Orc") == true => "orc"
      case _ if statement.contains("Parquet") == true => "parquet"
      case _ if statement.contains("Text") == true => "text"
      case _ => "other"
    }
  }

  /**
    * 数量统计
    *
    * @param spark
    * @param execTable
    * @return
    */
  def statistics(spark: SparkSession, execTable: String) = {
    spark.sql(s"select count(*) from ${execTable}").first().apply(0)
  }

  /**
    * 参数解析
    *
    * @param jobParams
    * @return
    */
  def parms(jobParams: JobParams) = {
    jobParams match {
      case _ if jobParams.cumstom != null && CombineUtil.isLegalDate(jobParams.cumstom) == true => "CustomCombine"
      case _ if jobParams.cumstom != null && CombineUtil.isNumeric(jobParams.cumstom.replace(",", "")) == true => "DirCombine"
      case _ if jobParams.cumstom == null => "DirCombine"
      case _ => throw new JobParamsException(s"${jobParams.dw} ${jobParams.cumstom} ${jobParams.jobId}  输入参数格式如下: " +
        s"1. 数仓 日期" +
        s"2. 数仓 日期 ID集合" +
        s"3. 数仓" +
        s"4. 数仓 ID集合")
    }
  }

  case class JobParams(dw: String, cumstom: String, jobId: String)

  /**
    * 初始化文件系统
    *
    * @return
    */
  def initFileSystem(): FileSystem = {
    jdbcOperator = new JDBCOperator()
    paramMap = jdbcOperator.getAppParams(this.getClass)
    val conf = paramMap.get(JobType.HDFS.getType)
    configuration = new Configuration
    configuration.set("fs.defaultFS", conf.get("fs.defaultFS"))
    configuration.set("dfs.client.use.datanode.hostname", conf.get("dfs.client.use.datanode.hostname"))
    configuration.set("fs.hdfs.impl", conf.get("fs.hdfs.impl"))
    try
      fileSystem = FileSystem.get(configuration)
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    jdbcOperator.close()
    fileSystem
  }


  override def initialize: Unit = {
    jdbcOperator = new JDBCOperator()
    paramMap = jdbcOperator.getAppParams(this.getClass)
    jobSubmitExecutors = Executors.newFixedThreadPool(executors)
    setParameterMap(paramMap.get(JobType.SPARK.getType).asScala)
    spark = initSparkSession()
    spark.sparkContext.setCheckpointDir(ckPath)
    logCollect = new LogCollect
    combineUtil = new CombineUtil
    logger.info("initialize success")
  }

  override def close: Unit = {
    spark.close()
    logCollect.close()
  }

}

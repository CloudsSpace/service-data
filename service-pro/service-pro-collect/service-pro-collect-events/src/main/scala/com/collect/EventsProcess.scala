package com.collect

import com.JDBCOperator

import java.sql.Date
import java.{lang, util}
import com.alibaba.fastjson
import com.alibaba.fastjson.JSON
import com.collect.outputformat.RDDMultipleAppendTextOutputFormat
import com.job.JobType
import com.sdk.SparkStreamingOperator
import com.util.date.{DateStyle, DateUtil}
import org.apache.hadoop.io.Text
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.HashPartitioner
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.{Set, mutable}

/**
  * 通过SparkStreaming创建DStream
  * 消费kafka event_topic用户行为数据
  * 对用户行为数据进行预处理
  * 根据事件类型,事件日期落地HDFS
  *
  * @Author: ysh
  * @Date: 2019/5/31 16:04
  * @Version: 1.0
  */
class EventsProcess extends Serializable {

  private[this] final val OUTPUT_PATH: String = "hdfs://bigdata-2:8020/user/eventData/events/"

  /**
    * 消费kafka event_topic用户行为数据
    * 对事件数据根据时间,项目,事件类型进行解析
    * 预处理数据后根据事件和日期写入HDFS不同路径
    *
    * @param stream SparkStreaming整合Kafka DStream
    */
  def pretreatment(stream: InputDStream[ConsumerRecord[String, String]]) {
    try {
      stream.foreachRDD(rdd => {

        if (!rdd.isEmpty()) {
          //获取当期rdd offset
          //val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          //解析json数据获取 project event time分别写入不同路径
          rdd.map(record => {
            val jsonObj: fastjson.JSONObject = JSON.parseObject(record.value())
            val eventTime: lang.Long = jsonObj.getLongValue("time")
            val nowTime: Long = System.currentTimeMillis()

            val endTime = nowTime + (1000 * 60 * 60 * 24L)
            val startTime = nowTime - (1000 * 60 * 60 * 24 * 3L)

            if (eventTime > startTime && eventTime < endTime) {
              val project = jsonObj.getOrDefault("project", "others")
              val event = jsonObj.getOrDefault("event", "others")

              val date = new Date(eventTime)
              val now = new Date(nowTime)

              val eventDate = DateUtil.getDate(date)
              var eventHour = DateUtil.getNowHour(date)

              val today = DateUtil.getDate(now)
              val yesterday = DateUtil.getYesterday(DateStyle.YYYY_MM_DD)
              val nowHour = DateUtil.getNowHour(now)

              //延迟数据写入-1文件
              if (!eventDate.equals(today) && !eventDate.equals(yesterday))
                eventHour = "-1"
              else if (eventDate.equals(yesterday) && !nowHour.equals("00"))
                eventHour = "-1"

              (s"${project}_${event}/${eventDate}/${eventHour}", record.value())
            } else {
              null
            }
            //自定义输出方式
          }).filter(x => x != null).partitionBy(new HashPartitioner(10)).saveAsHadoopFile(OUTPUT_PATH, classOf[Text], classOf[Text], classOf[RDDMultipleAppendTextOutputFormat])
          //数据写入后提交当前rdd offset
          //stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}

object EventsProcess extends SparkStreamingOperator {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private var jdbcOperator :JDBCOperator = _

  private val second: Long = 60L
  private val topics: Set[String] = Set("event")
  private var ssc: StreamingContext = _
  private var stream: InputDStream[ConsumerRecord[String, String]] = _

  def main(args: Array[String]): Unit = {
    initialize
    //执行
    new EventsProcess().pretreatment(stream)
    //启动
    logger.info("start...")
    ssc.start()
    ssc.awaitTermination()
  }

  import collection.JavaConverters._
  override def initialize {
    jdbcOperator = new JDBCOperator()
    val paramsMap = jdbcOperator.getAppParams(this.getClass)
    setParameterMap(paramsMap.get(JobType.SPARK.getType).asScala)

    val kafkaParams: mutable.Map[String, String] = paramsMap.get(JobType.KAFKA.getType).asScala
    ssc = initStreamingContext(second)
    stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String]
        (topics, kafkaParams))
    logger.info("initialize success")
  }

  override def close(): Unit = {}

}

package com.task

import com.JDBCOperator
import com.alibaba.fastjson.JSON
import com.job.JobType
import com.util.PropertiesUtil
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, OutputFileConfig, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.{DateTimeBucketAssigner, SimpleVersionedStringSerializer}
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.util.Properties
import java.util.concurrent.TimeUnit

object LogStream {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private var jdbcOperator: JDBCOperator = _
  private val topic: String = "event"
  val outputPath = "hdfs://192.168.247.10:9000/stream"
  private val ckPath: String = "hdfs://192.168.247.10:9000/tmp"
  private var env: StreamExecutionEnvironment = _
  private var kafkaProperties: Properties = _

  def main(args: Array[String]): Unit = {


    //    val stream = env.addSource(new FlinkKafkaConsumer[String]("iteblog", new SimpleStringSchema(), properties))
    //    stream.addSink(new MultipleTextOutputFormatSinkFunction[(String, String)]("hdfs:///user/iteblog/outputs/"))
    //kafka consumer
    //    val consumer: FlinkKafkaConsumer[UserPortrayal] = new FlinkKafkaConsumer[UserPortrayal](topic, new UserPortrayalSchema(), kafkaProperties)
    //    logger.info("initialize kafka consumer")
    //    val stream: DataStream[UserPortrayal] = env.addSource(consumer)
    //数据预处理,下沉
    //stream.countWindowAll(1000).apply(new PortrayalWindowFunction).addSink(esSink)
    //    stream.addSink(esSink)


    /*指定source*//*指定source*/
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(new FsStateBackend(ckPath))
    val stream = env.socketTextStream("localhost", 2222)
    val conf = env.getCheckpointConfig
    env.enableCheckpointing(1000)
    conf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)
    conf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    print("conf")
    tableEnv
    //    stream.map(x=>JSON.parseObject(x).get("@timestamp")).print
    //    val config = OutputFileConfig
    //      .builder()
    //      .withPartPrefix("prefix")
    //      .withPartSuffix(".ext")
    //      .build()
    //    print("config")
    //
    //    val rollingPolicy = DefaultRollingPolicy.create()
    //      .withMaxPartSize(1024 * 1024 * 120) // 设置每个文件的最大大小 ,默认是128M。这里设置为120M
    //      .withRolloverInterval(Long.MaxValue) // 滚动写入新文件的时间，默认60s。这里设置为无限大
    //      .withInactivityInterval(60 * 1000) // 60s空闲，就滚动写入新的文件
    //      .build();
    //
    //    val sink:String = StreamingFileSink
    //      .forRowFormat(new Path(path), new SimpleStringEncoder[String]("UTF-8"))
    //      .withBucketAssigner(new BucketAssigner[String, String] {
    //        def custom(element: String): String = {
    //          JSON.parseObject(element).get("alilogtype").toString
    //        }
    //
    //        def getBucketId(in: String, context: BucketAssigner.Context): String = {
    //          custom(in)
    //        }
    //
    //        def getSerializer: SimpleVersionedSerializer[String] = {
    //          SimpleVersionedStringSerializer.INSTANCE
    //        }
    //      })
    //      .withRollingPolicy(rollingPolicy) //设置文件滚动条件
    //      .withBucketCheckInterval(1000l) //设置检查点
    //      .withOutputFileConfig(config)
    //      .build();
    print("sink")
    // BucketAssigner --分桶策略 默认每小时生成一个文件夹
    // RollingPolicy -- 分件滚动策略
    // --withInactivityInterval --最近30分钟没有收到新的记录  withRolloverInterval --它至少包含 60 分钟的数据
    // --withMaxPartSize 文件大小达到多少


    //    val sink = StreamingFileSink.forRowFormat(new Path(outputPath), new SimpleStringEncoder[String]("UTF-8"))
    //      // 采用的是自定义的分桶类，把文件保存到某个表的目录下，按照hive分区目录命名方式生成文件名
    //      .withBucketAssigner(new BucketAssigner[String, String] {
    //        def custom(element: String): String = {
    //          element.split("\t", -1)(0)
    //        }
    //
    //        def getBucketId(in: String, context: BucketAssigner.Context): String = {
    //          custom(in)
    //        }
    //
    //        def getSerializer: SimpleVersionedSerializer[String] = {
    //          SimpleVersionedStringSerializer.INSTANCE
    //        }
    //      })
    //      .withRollingPolicy(DefaultRollingPolicy.create()
    //        .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
    //        .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
    //        .withMaxPartSize(1024 * 1024)
    //        .build())
    //      .build()
    //    print("=====执行=====")
    //    val data = stream.map(x => Array(JSON.parseObject(x.toString).get("@timestamp"), x).mkString("\t"))

    //    data.addSink(sink)
    env.execute("log")


  }

  /**
    * 初始化flink
    */
  def initialize {
    //配置文件读取
    jdbcOperator = new JDBCOperator()
    val paramMap = jdbcOperator.getAppParams(this.getClass)
    //配置文件读取
    kafkaProperties = PropertiesUtil.mapToProperties(paramMap.get(JobType.KAFKA.getType))

    //初始化flink
    env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setNumberOfExecutionRetries(3)

    val config = env.getCheckpointConfig
    env.enableCheckpointing(10000)
    config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)
    config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.setStateBackend(new FsStateBackend(ckPath))
    logger.info("initialize success")
  }


}

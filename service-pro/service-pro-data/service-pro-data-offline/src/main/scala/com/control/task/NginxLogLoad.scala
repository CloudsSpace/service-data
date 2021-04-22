package com.control.task

import com.alibaba.fastjson.JSON
import com.qupeiyin.data.operator.sdk.SparkSqlOperator

import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

class NginxLogLoad extends Serializable {

  case class table(uid: String, uc_id: String, from_app: String, msg_timestamp: String, referer: String, agent: String, method: String,
                   appversion: String, requesturi: String, `timestamp`: String, size: String, devicecode: String, slbip: String,
                   clientip: String, domain: String, serverip: String, responsetime: String, status: String, beat: String, topic: String,
                   `type`: String, metadata_version: String, hostname: String, name: String, beat_version: String, request_id: String,
                   strategy_id: String, data_id: String, scene_type: String, data_type: String, exp_id: String, tag: String, show_id: String, rec_type: String, course_id: String, create_time: String)


  val metaArray = Array("beat", "topic", "type", "version")
  val beatArray = Array("hostname", "name", "version")
  val dataArry = Array("request_id", "strategy_id", "data_id", "scene_type", "type", "exp_id", "tag", "show_id", "rec_type", "course_id", "album_id", "create_time")

  def jsonParse(spark: SparkSession, path: String,tableName:String) = {
    import spark.sqlContext.implicits._
    val src = spark.sparkContext.textFile(path)
    val data = src.map(line => {
      var arr = ArrayBuffer[String]()
      val json = JSON.parseObject(line)

      val message = JSON.parseObject(json.getOrDefault("message", "null").toString)
      val metadata = JSON.parseObject(json.getOrDefault("@metadata", "null").toString)
      val beatjson = JSON.parseObject(json.getOrDefault("beat", "null").toString)
      val request_body = new Regex("(?<=data=\\[)\\{.*\\}(?=\\])").findFirstIn(line).getOrElse("{\"\":\"\"}")
      val requestBody = JSON.parseObject(request_body.replace("\\", "")).toString
      val from_app = new Regex("(?<=from_app=)\\d+").findFirstIn(line).getOrElse("null")
      val uid = new Regex("(?<=uid=)\\d+").findFirstIn(line).getOrElse("null")
      val uc_id = new Regex("(?<=uc_id=)\\d+").findFirstIn(line).getOrElse("null")
      val timestamp = new Regex("(?<=timestamp=)\\d+").findFirstIn(line).getOrElse("null")
      arr += uid += uc_id += from_app += timestamp
      val msgArry = Array("referer", "agent", "method", "appversion", "requesturi", "url", "@timestamp", "size", "devicecode", "slbip", "clientip", "domain", "serverip", "responsetime", "status")
      val metaArray = Array("beat", "topic", "type", "version")
      val beatArray = Array("hostname", "name", "version")
      val requestArray = Array("request_id", "strategy_id", "data_id", "scene_type", "type", "exp_id", "tag", "show_id", "rec_type", "course_id", "album_id", "create_time")

      for (msg <- msgArry) {
        arr += message.getOrDefault(msg, "null").toString
      }
      for (meta <- metaArray) {
        arr += metadata.getOrDefault(meta, "null").toString
      }
      for (beat <- beatArray) {
        arr += beatjson.getOrDefault(beat, "null").toString
      }
      for (request <- requestArray) {
        arr += JSON.parseObject(requestBody).getOrDefault(request, "null").toString
      }
      arr
    })

    val dataframe = data.map(x => table(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12), x(13),
      x(14), x(15), x(16), x(17), x(18), x(19), x(20), x(21), x(22), x(23), x(24), x(25), x(26), x(27), x(28), x(29), x(30), x(31), x(32), x(33),
      x(34), x(35))).toDF
    dataframe.registerTempTable("midTable")
    spark.sql(s"INSERT OVERWRITE table ods.${tableName} select * from midTable").show()
  }

}

object NginxLogLoad extends SparkSqlOperator {
  //  var spark: SparkSession = _

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("NginxLogLoad")
      .config("spark.sql.warehouse.dir", "hdfs://bigdata-2:8020/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()
//    val path = "D:\\admin\\Desktop\\demotest"
    val default_brand_appStart = "hdfs://bigdata-2:8020/user/nginxLog/logdata/default_brand_appStart"
    val production_brand_appStart = "hdfs://bigdata-2:8020/user/nginxLog/logdata/production_brand_appStart"
    val default_nginx_web_log_appStart = "hdfs://bigdata-2:8020/user/nginxLog/logdata/default_nginx_web_log_appStart"
    val production_nginx_web_log_appStart = "hdfs://bigdata-2:8020/user/nginxLog/logdata/production_nginx_web_log_appStart"

    new NginxLogLoad().jsonParse(spark, default_brand_appStart,"default_brand_appStart")
    new NginxLogLoad().jsonParse(spark, production_brand_appStart,"production_brand_appStart")
    new NginxLogLoad().jsonParse(spark, default_nginx_web_log_appStart,"default_nginx_web_log_appStart")
    new NginxLogLoad().jsonParse(spark, production_nginx_web_log_appStart,"production_nginx_web_log_appStart")

  }

  override def initialize: Unit = {
  }

  override def close: Unit = {
  }
}

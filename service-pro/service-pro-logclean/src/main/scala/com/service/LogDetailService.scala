package com.service

import java.text.SimpleDateFormat
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/**
  * @function 埋点数据清洗
  * @create by liuhao at 2019/4/15
  */
object LogDetailService {
  /**
    * @function 数据清洗
    * @param rdd 输入的rdd数据
    * @return 清洗后的rdd数据
    */
  def dataClean(rdd: RDD[String]) = {
    val regex = new Regex("\\{\"msgs\":.*\\}]\\}")
    val textFile = rdd.filter(line => line.contains("[ERROR]")&& line.contains("prod")&&regex.findAllIn(line.toString).length > 0)
    val data = textFile.flatMap(cols => {
      val col = cols.split(" \\[ERROR\\] ")
      val timestr = col.apply(0)
      val jsonstrArray = col.apply(1).split("_\\{")
      val offsetNum = jsonstrArray.apply(0)
      val jsonform = "{" + jsonstrArray.apply(1)
      val timeArray = ArrayBuffer[String]()
      timeArray += timestr += offsetNum
      val json = JSON.parseObject(jsonform)
      val msg = json.get("msgs").toString
      val schema = Array("app_user_id", "vid", "time", "fromtype", "url", "referrer_url", "event", "type", "app_id", "channel_id", "device_id", "extra")
      val schemaArray = valueData(msg, schema, timeArray)
      schemaArray
    })
    data
  }
  /**
    * @function json解析
    * @param jsonform
    * @param schema
    * @return
    */
  def jsonDataArray(jsonform: String, schema: Array[String]) = {
    val regex1 = new Regex("\\{\"msgs\":.*\\}]\\}")
    var arr = new ArrayBuffer[Array[String]]()
    if(jsonform.contains("prod")){
      for (json <- regex1.findAllIn(jsonform)) {
        val jsonStr = JSON.parseObject(json).get("msgs")
        val jsonArray = JSON.parseArray(jsonStr.toString).toArray()
        jsonArray.map(x => {
          val json = JSON.parseObject(x.toString)
          val array = ArrayBuffer[String]()
          for (arr <- schema) {
            val value = json.getOrDefault(arr, "")
            array += fieldmatch(arr, value).toString
          }
          arr.append(array.toArray)
        })
      }
    }
    arr.toArray
  }

  /**
    * @function json数据解析+时间戳拼接
    * @param json      json数据
    * @param schema    字段顺序数组
    * @param timeArray 时间戳+批次
    * @return json数据取值
    */
  def valueData(json: String, schema: Array[String], timeArray: ArrayBuffer[String]) = {
    val jsonArray = JSON.parseArray(json).toArray
    jsonArray.map(x => {
      val json = JSON.parseObject(x.toString)
      val array = ArrayBuffer[AnyRef]()
      array ++= timeArray
      for (arr <- schema) {
        val value = json.getOrDefault(arr, "")
        array += fieldmatch(arr, value).toString
      }
      array.mkString("\t")
    })
  }

  /**
    * @function 对字段进行处理
    */
  val reg = new Regex("\"")
  def fieldmatch(field: String, value: AnyRef) = field match {
    case "time" => if (reg.findAllIn(value.toString) == false) {
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(value)
    } else {
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(value.toString.replace("\"", "").toDouble)
    }
    case "extra" => if (value.toString.length > 2) {
      JSON.parseObject(String.valueOf(value)).getOrDefault("id", "")
    } else {
      ""
    }
    //case "extra" => JSON.parseObject(String.valueOf(value)).getOrDefault("id","")
    case _ => value
  }
}

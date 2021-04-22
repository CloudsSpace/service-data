package com

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSONObject
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{CompareFilter, RowFilter, SubstringComparator}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object HbaseUtil {
  @transient lazy val logger: Logger = Logger.getLogger(this.getClass().getName())
  //参数
  val tableName: String = "orderTable"
  val quorum = "ddx,ddy,ddz"//gaiaa,gaiab,gaiac,gaiad,gaiae
  val master = "ddx"
  /**
    * @function 获取hbase配置
    * @return
    */
  def getHbaseConf(): Configuration = {
    var hbaseConf: Configuration = null
    try {
      //获取hbase的conf
      hbaseConf = HBaseConfiguration.create()
      //设置写入的表
      hbaseConf.set("hadoop.home.dir", "/d1/bin/hadoop")
      hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
      hbaseConf.set("hbase.zookeeper.quorum", quorum) // 47.97.206.57,47.97.183.128,47.96.3.154,47.99.131.49,47.98.106.110
      hbaseConf.set("hbase.master", s"${master}:60010")
      hbaseConf.set("hbase.client.pause", "500")
      hbaseConf.set("hbase.client.retries.number", "500")
      hbaseConf.set("hbase.rpc.timeout", "20000000")
      hbaseConf.set("hbase.client.operation.timeout", "300000")
      hbaseConf.set("hbase.client.scanner.timeout.period", "100000")
    } catch {
      case e: Exception => logger.error(">>>连接hbase失败:," + e)
    }
    hbaseConf
  }

  /**
    * @function 获得操作类Table
    * @param tableName
    * @return
    */
  def getTable(tableName: String): Table = {
    var table: Table = null
    try {
      val hbaseConf = getHbaseConf()
      val conn = ConnectionFactory.createConnection(hbaseConf)
      table = conn.getTable(TableName.valueOf(tableName))
    } catch {
      case e: Exception => logger.error(">>>获取Table对象失败:" + e)
    }
    table
  }

  /**
    * @function 添加一行数据
    * @param table  操作表的对象
    * @param rowKey key
    * @param family 簇
    * @param column 列
    * @param value  需要添加的数据内容
    */
  def addRow(table: Table, rowKey: String, family: String, column: String, value: String): Unit = {
    val rowPut: Put = new Put(Bytes.toBytes(rowKey))
    if (value == null) {
      rowPut.addColumn(family.getBytes, column.getBytes, "".getBytes)
    } else {
      rowPut.addColumn(family.getBytes, column.getBytes, value.getBytes)
    }
    table.put(rowPut)
  }


  /**
    * @function rdd数据入到hbase库
    * @param inputRdd 需要入库的RDD
    */
  def putRdd(inputRdd: RDD[JSONObject]): Unit = {
    try {
      val jobConf = new JobConf(getHbaseConf())
      jobConf.setOutputFormat(classOf[TableOutputFormat])
      jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
      val putRdd = inputRdd.map(convert)
      putRdd.saveAsHadoopDataset(jobConf)
    } catch {
      case e: Exception => logger.error(">>>数据入库失败:" + e)
    }
  }

  /**
    * @function 构建转换数据
    * @param obj
    * @return
    */
  def convert(obj: JSONObject) = {
    var put: Put = null
    try {
      //设置主键，这里测试，使用系统时间
      val longtTime = System.currentTimeMillis()
      val dfTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")
      val saveTime = dfTime.format(new Date(longtTime))
      val key = saveTime
      put = new Put(Bytes.toBytes(key))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("date"), Bytes.toBytes(saveTime))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("wrong_value"), Bytes.toBytes(obj.toString))
    } catch {
      case e: Exception => println(">>>数据转换失败：" + e)
    }
    (new ImmutableBytesWritable, put)
  }

  /**
    * @function 根据rowKey精确查询单行数据单行值
    * @param rowkey
    * @param family
    * @param column
    * @return
    */
  def getByKey(table: Table, rowkey: String, family: String, column: String): String = {
    var value: String = null
    try {
      //单个get查询
      val get = new Get(Bytes.toBytes(rowkey))
      get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column))
      val result = table.get(get)
      value = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(column)))
    } catch {
      case e: Exception => logger.error(">>>查询失败:" + e)
    }
    value
  }

  /**
    * @function 根据rowKey精确查询单行数据的多列值
    * @param rowkey
    * @param family
    * @param columns
    * @return
    */
  def getByKey1(table: Table, rowkey: String, family: String, columns: Array[String]): ArrayBuffer[String] = {
    var list: ArrayBuffer[String] = new ArrayBuffer[String]()
    try {
      val get = new Get(Bytes.toBytes(rowkey))
      get.addFamily(family.getBytes())
      val result = table.get(get)
      columns.foreach(column => {
        val value = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(column)))
        list += value
      })
    } catch {
      case e: Exception => logger.error(">>>查询失败:" + e)
    }
    list
  }

  /**
    * @function 根据subKey模糊批量查询多行数据
    * @param subKey
    * @param family
    * @param column
    * @return
    */
  def scanBySubKey(table: Table, subKey: String, family: String, column: String): ArrayBuffer[String] = {
    val resultList = new ArrayBuffer[String]()
    val table = getTable(tableName)
    //批量模糊查询
    val filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(subKey))
    val scan = new Scan()
    scan.setFilter(filter)
    val rs: ResultScanner = table.getScanner(scan)
    for (result <- rs) {
      if (result.containsColumn(Bytes.toBytes(family), Bytes.toBytes(column))) {
        val value = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(column)))
        resultList += value
      }
    }
    resultList
  }

  /**
    * @function 根据rowKey删除整行
    * @param table
    * @param rowKey
    */
  def deleteByKey(table: Table, rowKey: String): Unit = {
    try {
      table.delete(new Delete(rowKey.getBytes()))
    } catch {
      case e: Exception => println(">>>删除操作失败：" + e)
    }
  }

  /**
    * @function 删除rowKey的某簇的某列
    * @param table
    * @param rowKey
    */
  def deleteByColumn(table: Table, rowKey: String, family: String, column: String): Unit = {
    try {
      val delete = new Delete(rowKey.getBytes())
      delete.addColumn(family.getBytes(), column.getBytes())
      table.delete(delete)
    } catch {
      case e: Exception => println(">>>删除操作失败：" + e)
    }
  }
  /**
    * @function 删除rowKey的某个family
    * @param table
    * @param rowKey
    */
  def deleteByFamily(table: Table, rowKey: String, family: String): Unit = {
    try {
      val delete = new Delete(rowKey.getBytes())
      delete.addFamily(family.getBytes())
      table.delete(delete)
    } catch {
      case e: Exception => println(">>>删除操作失败：" + e)
    }
  }

  /**
    * @function 根据rowKey模糊删除
    * @param table
    * @param subKey
    */
  def deleteByKeys(table: Table, subKey: String): Unit = {
    try {
      //批量模糊删除
      val filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(subKey))
      val scan = new Scan()
      scan.setFilter(filter)
      val scanners: ResultScanner = table.getScanner(scan)
      val keyList = new ArrayBuffer[Delete]()
      var delete: Delete = null
      //注意：scala和java的for循环有区别，需要引入转换  import scala.collection.JavaConversions._
      for (scanner <- scanners) {
        val rowKey = new String(scanner.getRow)
        delete = new Delete(rowKey.getBytes)
        keyList += delete
      }
      table.delete(keyList)
    } catch {
      case e: Exception => println(">>>删除操作失败：" + e)
    }
  }
  /**
    * @function 读取hbase数据
    * @param servers
    * @param sc
    * @param table
    */
//  def loadHbase(servers: String, sc: SparkContext, table: String) = {
//    val hbaseConf = HBaseConfiguration.create()
//    hbaseConf.set("hbase.zookeeper.quorum", servers)
//    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
//    hbaseConf.set("zookeeper.session.timeout", "6000000")
//    hbaseConf.set(TableInputFormat.INPUT_TABLE, table)
//    val ratingsData = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
//    val hbaseRatings = ratingsData.map { case (_, res) =>
//      val app_user_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("app_user_id")))
//      val extra_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("extra_id")))
//      val count = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("count")))
//      Array(extra_id.toInt, count.toDouble).mkString("\t")
//    }
//  }

}

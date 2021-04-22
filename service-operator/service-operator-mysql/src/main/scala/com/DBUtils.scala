package com

import java.sql.{Connection, DriverManager}
import java.util.Properties
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

object DBUtils {
  /**
    * @function 单分区模式
    * @param sqlContext
    * @return
    */
  def loadDB(sqlContext: SQLContext, prop: Properties) = {
    sqlContext.read.format("jdbc").option("url", prop.getProperty("jdbc.url"))
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", prop.getProperty("jdbc.table"))
      .option("user", prop.getProperty("jdbc.user"))
      .option("password", prop.getProperty("jdbc.password"))
      .load()
  }

  /**
    * @function 多分区模式
    * @param sqlContext
    * @param prop
    * @param column
    * @return
    */
  def loadDB(sqlContext: SQLContext, prop: Properties, column: String) = {
    sqlContext.read.format("jdbc").option("url", prop.getProperty("jdbc.url"))
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", prop.getProperty("jdbc.table"))
      .option("user", prop.getProperty("jdbc.user"))
      .option("password", prop.getProperty("jdbc.password"))
      .option("partitionColumn", column)
      .option("lowerBound", 1)
      .option("upperBound", 10000)
      .option("numPartitions", 5)
      .option("fetchsize", 100)
      .load()
  }

  /**
    * @function 分页模式读取（高自由度分区）
    * @return
    */
  val partitons = new Array[String](3)
  partitons(0) = "1=1 limit 0,10000"
  partitons(1) = "1=1 limit 10000,10000"
  partitons(2) = "1=1 limit 20000,10000"

  def loadD(sqlContext: SQLContext, prop: Properties, predicates: Array[String]) = {
    val pro = new Properties()
    pro.setProperty("user", prop.getProperty("jdbc.user"))
    pro.setProperty("password", prop.getProperty("jdbc.password"))
    sqlContext.read.jdbc(prop.getProperty("jdbc.url"), prop.getProperty("jdbc.table"), predicates, pro)
  }

  /**
    * @function 写数据库
    * @param data
    * @param prop
    */
  def saveDB(data: DataFrame, prop: Properties) = {
    val pro = new Properties()
    pro.setProperty("user", prop.getProperty("jdbc.user"))
    pro.setProperty("password", prop.getProperty("jdbc.password"))
    data.write.mode(SaveMode.Overwrite).jdbc(prop.getProperty("jdbc.url"), prop.getProperty("jdbc.table"), pro)
  }

  /**
    * @function 写数据库
    * @param data
    * @param url
    * @param table
    * @param user
    * @param password
    */
  def saveDB(data: DataFrame, url: String, table: String, user: String, password: String) = {
    val prop = new Properties()
    prop.setProperty("user", user)
    prop.setProperty("password", password)
    data.write.mode(SaveMode.Append).jdbc(url, table, prop)
  }

  /**
    * @function
    * @param partition
    * @param prop
    * @param sql
    */
  def saveToMysql(partition: Iterator[Row], prop: Properties, sql: String): Unit = {
    var connect: Connection = null
    Class.forName("com.mysql.jdbc.Driver")
    connect = DriverManager.getConnection(prop.getProperty("jdbc.url"), prop.getProperty("jdbc.user"), prop.getProperty("jdbc.password"))
    connect.setAutoCommit(false)
    val pstmt = connect.prepareStatement(sql)
    partition.foreach(line => {
      pstmt.setString(1, line.getString(0))
      pstmt.setInt(2, line.getInt(1))
      pstmt.setString(3, line.getString(2))
      pstmt.setString(4, line.getString(3))
      pstmt.setString(5, line.getString(4))
      pstmt.setString(6, line.getString(5))
      pstmt.setString(7, line.getString(6))
      pstmt.setString(8, line.getString(7))
      pstmt.setString(9, line.getString(8))
      pstmt.setString(10, line.getString(9))
      pstmt.setString(11, line.getString(10))
      pstmt.setString(12, line.getString(11))
      pstmt.setString(13, line.getString(12))
      pstmt.setString(14, line.getString(13))
      pstmt.addBatch()
    })
    var result = pstmt.executeBatch()
    if (result == 1) {
      println("写入mysql成功.............")
    }
  }

  /**
    * @function 写入sqlServer
    * @param dataFrame
    * @param table
    * @param prop
    */
  def saveSqlServer(dataFrame: DataFrame, table: String, prop: Properties) = {
    dataFrame.write.mode(SaveMode.Append).format("jdbc")
      .option("url", prop.getProperty("url"))
      .option("dbtable", table)
      .option("user", prop.getProperty("user"))
      .option("password", prop.getProperty("password"))
      .save()
  }


}

package com

import java.sql._

object JDBCUtil {
  private val url = ""
  private val user = ""
  private val password = ""
  private var driverClass=""
  private val threadLocal:ThreadLocal[Connection]=new ThreadLocal

  //注册驱动
  Class.forName(driverClass)

  //获取连接并放入连接池
  private def getConnection() = {
    var connection = threadLocal.get()
    if (connection == null) {
      connection = DriverManager.getConnection(url, user, password)
      threadLocal.set(connection)
    }
    connection
  }

  //关闭连接并释放连接池
  private def closeConnection() {
    val connection = threadLocal.get()
    if (connection != null) {
      connection.close()
    }
    threadLocal.remove()
  }

  def executeQuery(sql: String, database: String*): Set[Map[String, Object]] = {
    var conn: Connection = null
    var stat: Statement = null
    var resultSet: ResultSet = null
    var result = Set[Map[String, Object]]()
    try {
      conn = getConnection()
      stat = conn.createStatement()
      if(database!=null&&database.size>0){
        var useSql="use "
        useSql+=database(0)
        stat.executeUpdate(useSql)
      }
      resultSet = stat.executeQuery(sql)
      val rsm: ResultSetMetaData = resultSet.getMetaData()
      val colNum: Int = rsm.getColumnCount()
      while (resultSet.next()) {
        var map = Map[String, Object]()
        for (i <- 0 to colNum - 1) {
          map += (rsm.getColumnName(i + 1).toUpperCase() -> resultSet.getObject(i + 1))
        }
        result += map
      }
    } catch {
      case e: Exception =>
        throw new Exception(e);
    } finally {
      if (resultSet != null) {
        resultSet.close();
      }
      if (stat != null) {
        stat.close();
      }
      if (conn != null) {
        closeConnection();
      }
    }
    result
  }
}

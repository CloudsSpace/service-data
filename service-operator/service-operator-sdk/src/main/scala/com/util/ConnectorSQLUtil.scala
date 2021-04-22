package com.util

/**
  * @Classname ConnectorSQLUtil
  * @Description TODO
  * @Date 2020/3/14 14:01
  * @Created by ysh
  */
object ConnectorSQLUtil {

  def jdbcWriteSQL(desc:String,url:String,table:String,driver:String,username:String,password:String,maxRow:Int,flushInterval:String,maxRetries:Int ): String = {
    val jdbcSQL = s"CREATE TABLE $table ($desc) " +
      "WITH (  'connector.type' = 'jdbc'," +
      s" 'connector.url' = '$url'," +
      s" 'connector.table' = '$table'," +
      s" 'connector.driver' = '$driver'," +
      s" 'connector.username' = '$username', " +
      s" 'connector.password' = '$password', " +
      s" 'connector.write.flush.max-rows' = '$maxRow'," +
      s" 'connector.write.flush.interval' = '$flushInterval'," +
      s" 'connector.write.max-retries' = '$maxRetries')"

    jdbcSQL
  }

}

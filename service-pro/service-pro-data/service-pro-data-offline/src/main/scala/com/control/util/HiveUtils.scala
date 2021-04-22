package com.control.util

import org.apache.spark.sql.types.StructField

import scala.collection.mutable

object HiveUtils {

  def createTable(tableName: String, fields: Array[StructField]): String = {
    val create: StringBuilder = new mutable.StringBuilder(s"create EXTERNAL table if not exists ods.${tableName}  (")
    for (field <- fields) {
      val filedName = field.name
      val dataType = field.dataType.simpleString
      create.append(s"`${filedName}` ${dataType}, ")
    }
    create.append(s") partitioned by (staticdate string)  stored as orc LOCATION 'hdfs://nameservice1/user/hive/warehouse/ods.db/${tableName}'")
    val sql = create.toString().replace(", )", ")")
    sql
  }

}

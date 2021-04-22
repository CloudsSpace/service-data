package com.sdk

import scala.collection.mutable

/**
  *
  * @Author: ysh
  * @Date: 2019/10/21 18:16
  * @Version: 1.0
  */
trait Operator extends Serializable {

  private var parameterMap: mutable.Map[String, String] = _

  def setParameterMap(parameterMap: mutable.Map[String, String]) {
    this.parameterMap = parameterMap
  }

  def getParameterMap: mutable.Map[String, String] = parameterMap

  def initialize

  def close
}

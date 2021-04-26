package com.duiba.util

import scala.collection.mutable

trait Operator extends Serializable {

  private var parameterMap: mutable.Map[String, String] = _

  def setParameterMap(parameterMap: mutable.Map[String, String]) {
    this.parameterMap = parameterMap
  }

  def getParameterMap: mutable.Map[String, String] = parameterMap

  def initialize

  def close
}

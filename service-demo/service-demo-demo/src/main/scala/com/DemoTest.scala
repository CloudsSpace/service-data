package com

import java.util.concurrent.ExecutorService

object DemoTest{
  private var jobSubmitExecutors: ExecutorService = _

  def main(args: Array[String]): Unit = {

    def mul(x:Int,y:Int) = x * y  //该函数接受两个参数
    def mulOneAtTime(x:Int) = (y:Int) => x * y  //该函数接受一个参数生成另外一个接受单个参数的函数
    println(mulOneAtTime(5)(4))

  }
}

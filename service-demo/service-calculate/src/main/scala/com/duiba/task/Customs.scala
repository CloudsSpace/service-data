package com.duiba.task

import org.apache.spark.{SparkConf, SparkContext}

object Customs {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("appName").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val input = sc.textFile("/Users/admin/workspace/idea/service-data/service-demo/service-calculate/data/hologress.csv")
    val res = input.map(line => {
      val arr = line.split("\t", -1)
      (arr(1), arr(0) + " " + arr(2))
    }).collect().toMap

    val str = "date_partition,md5_string,md5_string,account_name,advert_id,advert_name,activity_type,app_id,app_name,slot_id,slot_name,slot_tag,package_id,package_name,put_index,new_trade,ua,is_fee,consumer_total,launch_count,ef_click,expose_pv,change_pv,tag_flow_title,trade_tag,platform,app_keywords,app_tag_num,download_advert,exposure_count,start_pv,registe_pv,activate_pv,pay_pv,entry_pv,finish_pv,reveive_pv,material_id,resoure_name,request_pv,activity_delivery_type,activate_uv,activate_uv,promote_url,charge_type,activity_subactivity_way,depth_convert_subtype,convert_subtype,orientation_app,puttarget_type,tag1,tag2,ding_pv,fit_pv"
    val key = str.split(",", -1)
      for (k <- key) {

        val arr = res.getOrElse(k, "nulldata").split(" ", -1)

        println("{ index :"+(arr(0).toInt-1)+","+" type : "+arr(1)+" },")
      }


  }

}

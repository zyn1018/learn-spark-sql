package com.yinan.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * HiveContext使用
  */
object HiveContextApp {
  def main(args: Array[String]): Unit = {

    //1. 创建相应的context
    val sparkConf = new SparkConf

    //在测试或生产中，AppName和Master是通过脚本指定的
    //sparkConf.setAppName("HiveContextApp").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    //2. 相关处理: json
    hiveContext.table("emp").show();

    //3. 关闭资源
    sc.stop()
  }
}

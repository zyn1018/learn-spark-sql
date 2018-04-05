package com.yinan.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * SQLContext使用
  */
object SQLContextApp {
  def main(args: Array[String]): Unit = {

    val path = args(0)

    //1. 创建相应的context
    val sparkConf = new SparkConf

    //在测试或生产中，AppName和Master是通过脚本指定的
    //sparkConf.setAppName("SQLContextApp").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    //2. 相关处理: json
    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()

    //3. 关闭资源
    sc.stop()
  }
}

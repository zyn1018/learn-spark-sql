package com.yinan.spark

import org.apache.spark.sql.SparkSession

object SparkSessionApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSessionApp").master("local[2]").getOrCreate()

    val people = spark.read.json(args(0))
    people.show()
    spark.stop()
  }
}

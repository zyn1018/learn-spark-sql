package com.yinan.spark

import org.apache.spark.sql.SparkSession


object ParquetApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ParquetApp").master("local[2]").getOrCreate()

    spark.read.format("parquet").load()

    spark.stop()
  }
}

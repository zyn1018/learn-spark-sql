package com.yinan.spark

import org.apache.spark.sql.SparkSession

/**
  * Schema Infer
  */
object SchemaInferApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SchemaInferApp").master("local[2]").getOrCreate()
    spark.read.format("json").load("")
    spark.stop()
  }
}

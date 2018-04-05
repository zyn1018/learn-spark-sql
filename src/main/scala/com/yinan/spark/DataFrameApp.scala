package com.yinan.spark

import org.apache.spark.sql.SparkSession

/**
  * DataFrame ApI
  */
object DataFrameApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameApp").master("local[2]").getOrCreate()

    //将json加载成dataframe
    val peopleDF = spark.read.format("json").load("/Users/yinan/Desktop/people.json");

    //打印DataFrame对应的Schema
    peopleDF.printSchema()

    //输出数据集
    peopleDF.show()

    //只输出某一列
    peopleDF.select("name").show()

    peopleDF.select(peopleDF.col("name"), peopleDF.col("age") + 10).show()

    peopleDF.filter(peopleDF.col("age")>19).show()

    peopleDF.groupBy(peopleDF.col("age")).count().show()
    spark.stop()
  }
}

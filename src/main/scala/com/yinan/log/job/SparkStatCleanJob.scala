package com.yinan.log.job

import com.yinan.log.utils.LogConverterUtil
import org.apache.spark.sql.SparkSession

/**
  * 数据清洗
  */
object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()
    val accessRDD = spark.sparkContext.textFile("file:///Users/yinan/Documents/BigData/SparkSQL/data/access.log")

    //RDD->DataFrame
    val accessDF = spark.createDataFrame(accessRDD.map(x => LogConverterUtil.parseLog(x)), LogConverterUtil.struct)

    //coalesce控制输出文件的分区数
    accessDF.coalesce(1).write.format("parquet").partitionBy("day").save("file:///Users/yinan/Documents/BigData/SparkSQL/data/clean")
    spark.stop()
  }
}

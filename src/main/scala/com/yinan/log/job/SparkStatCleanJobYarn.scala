package com.yinan.log.job

import com.yinan.log.utils.LogConverterUtil
import org.apache.spark.sql.SparkSession

/**
  * 数据清洗 on Yarn
  */
object SparkStatCleanJobYarn {
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      print("Usage: SparkStatCleanJobYarn <inputPath> <outputPath>")
      System.exit(1)
    }
    val Array(inputPath, outputPath) = args
    val spark: SparkSession = SparkSession.builder().getOrCreate()
    val accessRDD = spark.sparkContext.textFile(inputPath)

    //RDD->DataFrame
    val accessDF = spark.createDataFrame(accessRDD.map(x => LogConverterUtil.parseLog(x)), LogConverterUtil.struct)

    //coalesce控制输出文件的分区数
    accessDF.coalesce(1).write.format("parquet").partitionBy("day").save(outputPath)
    spark.stop()
  }
}

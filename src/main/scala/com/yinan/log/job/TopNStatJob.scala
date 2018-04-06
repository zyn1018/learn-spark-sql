package com.yinan.log.job

import com.yinan.log.dao.StatDao
import com.yinan.log.model.{DayCityVideoAccessStat, DayVideoAccessStat, DayVideoTrafficsStat}
import org.apache.spark.sql
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object TopNStatJob {

  /**
    * 最受欢迎的课程TOP N
    *
    * @param spark
    * @param accessDF
    */
  def videoAccessTopNStat(spark: SparkSession, accessDF: sql.DataFrame): Unit = {

    // via DataFrame API
    import spark.implicits._
    val videoAccessTopNDF = accessDF.groupBy("day", "cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)

    // via SQL
    //    accessDF.createOrReplaceTempView("access_logs")
    //    val videoAccessTopNDF = spark.sql("select day, cmsId, count(*) as times from access_logs " +
    //      "where day='20170511' and cmsType='video' group by day, cmsId order by count(*) desc")

    try {
      videoAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoAccessStat]
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")

          list.append(DayVideoAccessStat(day, cmsId, times))
        })
        StatDao.insertDayVideoAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * 每个城市最受欢迎的课程v
    *
    * @param spark
    * @param accessDF
    * @return
    */
  def cityAccessTopNStat(spark: SparkSession, accessDF: DataFrame): Unit = {
    // via DataFrame API
    val cityAccessTopNDF = accessDF.groupBy("day", "city", "cmsId").agg(count("cmsId").as("times"))

    // Window in Spark SQL
    val cityTopDF = cityAccessTopNDF.select(cityAccessTopNDF("day"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopNDF("city"))
        .orderBy(cityAccessTopNDF("times").desc)).as("times_rank")
    ).filter("times_rank <= 3")

    try {
      cityTopDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayCityVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val city = info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val timesRank = info.getAs[Int]("times_rank")
          list.append(DayCityVideoAccessStat(day, cmsId, city, times, timesRank))
        })

        StatDao.insertDayCityVideoAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def videoTrafficsTopNStat(spark: SparkSession, accessDF: DataFrame): Unit = {
    import spark.implicits._
    val trafficAccessTopNDF = accessDF.groupBy("day", "cmsId").agg(sum("traffic").as("traffics")).orderBy($"traffics".desc)
    try {
      trafficAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoTrafficsStat]
        for (elem <- partitionOfRecords) {
          val day = elem.getAs[String]("day")
          val cmsId = elem.getAs[Long]("cmsId")
          val traffics = elem.getAs[Long]("traffics")
          list.append(DayVideoTrafficsStat(day, cmsId, traffics))
        }
        StatDao.insertDayVideoTrafficsAccessTopN(list)
      })

    } catch {
      case e: Exception => e.printStackTrace()
    }

  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", value = false).getOrCreate()
    val accessDF = spark.read.format("parquet").load("file:///Users/yinan/Documents/BigData/SparkSQL/data/clean")
    val day = "20170511"

    import spark.implicits._
    val commonDF = accessDF.filter($"day" === day && $"cmsType" === "video")
    commonDF.cache()
    StatDao.deleteDataByDay(day)
    //最受欢迎的课程（访问量最高）
    videoAccessTopNStat(spark, commonDF)

    //按照城市进行统计TopN课程
    cityAccessTopNStat(spark, commonDF)

    //按照流量进行统计
    videoTrafficsTopNStat(spark, commonDF)

    commonDF.unpersist(true);
    spark.stop()
  }

}

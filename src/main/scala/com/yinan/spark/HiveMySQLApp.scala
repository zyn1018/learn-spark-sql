package com.yinan.spark

import org.apache.spark.sql.SparkSession

/**
  * 使用外部数据源综合查询Hive和MySQL的表数据
  */
object HiveMySQLApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("HiveMySQLApp").master("local[2]").getOrCreate()

    //加载hive表数据
    val hiveDF = spark.table("emp")

    //加载mysql数据
    val mysqlDF = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306").option("dbtable","spark.DEPT").option("user","root").option("password","root").option("driver","com.mysql.jdbc.Driver").load()

    //JOIN
    val resultDF = hiveDF.join(mysqlDF, hiveDF.col("deptno") === mysqlDF.col("DEPTNO"))

    //Show result
    resultDF.show()

    spark.stop()
  }
}

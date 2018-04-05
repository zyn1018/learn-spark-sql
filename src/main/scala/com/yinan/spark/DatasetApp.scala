package com.yinan.spark

import org.apache.spark.sql.SparkSession

object DatasetApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DatasetApp").master("local[2]").getOrCreate()

    //导入隐式转换
    import spark.implicits._

    val path = "file:///Users/yinan/Documents/BigData/SparkSQL/data/sales.csv"

    val saleDF = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    saleDF.show()

    val ds = saleDF.as[Sales]

    ds.map(line => line.itemId).show
  }

  case class Sales(transactionId: Int, customerId: Int, itemId: Int, amountPaid: Double)

}

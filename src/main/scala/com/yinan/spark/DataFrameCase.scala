package com.yinan.spark

import org.apache.spark.sql.SparkSession

/**
  * DataFrame中的其他操作
  */
object DataFrameCase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameCase").master("local[2]").getOrCreate()
    val rdd = spark.sparkContext.textFile("/Users/yinan/Documents/BigData/SparkSQL/data/student.data")

    import spark.implicits._
    val studentDF = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF
    val studentDF2 = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF

    studentDF.show(23, truncate = false)

    studentDF.take(10)

    studentDF.head(3)

    studentDF.filter("name=''").show()
    studentDF.filter("name='' OR name='NULL'").show()

    studentDF.filter("SUBSTR(NAME, 0, 1) ='M'").show()

    //可以按照单个或多个字段排序
    studentDF.sort(studentDF.col("name")).show()

    //重命名
    studentDF.select(studentDF("name").as("student_name")).show()

    //join
    studentDF.join(studentDF2, studentDF("id") === studentDF2("id")).show()
    spark.stop()
  }

  case class Student(id: Int, name: String, phone: String, email: String)

}

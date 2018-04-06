package com.yinan.spark

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 日期时间解析工具类
  *  注：SimpleDateFormat线程不安全
  */
object DataUtils {
  val YYYYMMDDHHMM_TIME_FORMAT: FastDateFormat = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)

  val TARGET_FORMAT: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  /**
    * 将时间转成目标格式（TARGET_FORMAT）
    *
    * @param time
    */
  def parse(time: String): String = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  /**
    * 将输入时间转成long类型
    *
    * @param time
    * @return
    */
  def getTime(time: String): Long = {
    try {
      YYYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1,
        time.lastIndexOf("]"))).getTime
    } catch {
      case e: Exception => {
        0l
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(parse("[11/Nov/2016:12:01:02 +0800]"))
  }
}

package com.yinan.log.dao

import java.sql.{Connection, PreparedStatement}

import com.yinan.log.model.{DayCityVideoAccessStat, DayVideoAccessStat, DayVideoTrafficsStat}
import com.yinan.log.utils.MySQLUtil

import scala.collection.mutable.ListBuffer

/**
  * 统计的DAO操作
  */
object StatDao {

  /**
    * 批量保存DayVideoAccessStat到数据库
    *
    * @param list
    */
  def insertDayVideoAccessTopN(list: ListBuffer[DayVideoAccessStat]): Unit = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MySQLUtil.getConnection()

      //设置手动提交
      connection.setAutoCommit(false)

      val sql = "insert into day_video_access_topn_stat(day, cms_id, times) values (?,?,?)"
      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.times)
        //批量处理，执行更快
        pstmt.addBatch()
      }
      pstmt.executeBatch()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtil.stop(connection, pstmt)
    }
  }

  /**
    * 批量保存DayCityVideoAccessStat到数据库
    *
    * @param list
    */
  def insertDayCityVideoAccessTopN(list: ListBuffer[DayCityVideoAccessStat]): Unit = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MySQLUtil.getConnection()

      //设置手动提交
      connection.setAutoCommit(false)

      val sql = "insert into day_video_city_access_topn_stat(day,cms_id,city,times, times_rank) values (?,?,?,?,?)"
      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setString(3, ele.city)
        pstmt.setLong(4, ele.times)
        pstmt.setInt(5, ele.timesRank)
        //批量处理，执行更快
        pstmt.addBatch()
      }
      pstmt.executeBatch()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtil.stop(connection, pstmt)
    }
  }

  def insertDayVideoTrafficsAccessTopN(list: ListBuffer[DayVideoTrafficsStat]): Unit = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MySQLUtil.getConnection()
      connection.setAutoCommit(false)

      val sql = "insert into day_video_traffics_topn_stat (day,cms_id,traffics) values (?,?,?)"
      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.traffics)
        pstmt.addBatch()
      }
      pstmt.executeBatch()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtil.stop(connection, pstmt)
    }
  }

  def deleteDataByDay(day: String): Unit = {
    val tables = Array("day_video_access_topn_stat", "day_video_city_access_topn_stat", "day_video_traffics_topn_stat")
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySQLUtil.getConnection()
      for (table <- tables) {
        val deleteSQL = s"delete from $table where day=?"
        pstmt = connection.prepareStatement(deleteSQL)
        pstmt.setString(1, day)
        pstmt.executeUpdate()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtil.stop(connection, pstmt)
    }
  }
}

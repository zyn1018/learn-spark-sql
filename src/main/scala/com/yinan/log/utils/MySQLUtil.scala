package com.yinan.log.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

object MySQLUtil {
  /**
    * 获取数据库连接
    */
  def getConnection(): Connection = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_test", "root", "zyn123zyn")
  }

  /**
    * 释放资源
    *
    * @param connection
    * @param pstmt
    */
  def stop(connection: Connection, pstmt: PreparedStatement): Unit = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }
}

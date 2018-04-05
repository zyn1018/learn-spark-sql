package com.yinan.spark

import java.sql.DriverManager

object SparkSQLThriftServerApp {
  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")

    val conn = DriverManager.getConnection("jdbc:hive2://192.168.1.14:10000", "hadoop", "")
    val pstmt = conn.prepareStatement("select empno, ename, sal from emp")

    val rs = pstmt.executeQuery()

    while (rs.next()) {
      print("empno: " + rs.getInt("empno") + ", ename: " + rs.getString("ename") + ", salary: " + rs.getDouble("sal"))
    }
    rs.close()
    pstmt.close()
    conn.close()
  }
}

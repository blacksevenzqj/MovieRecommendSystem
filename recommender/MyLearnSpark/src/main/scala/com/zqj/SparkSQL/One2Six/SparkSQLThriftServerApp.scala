package com.zqj.SparkSQL

import java.sql.DriverManager


// Hive的表 是区分 库 的，但不同库的 相同表名 的数据 是 共享的。
object SparkSQLThriftServerApp {

  def main(args: Array[String]): Unit ={

    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val conn =  DriverManager.getConnection("jdbc:hive2://hadoop104:10000", "hadoop", "")
    val pstmt = conn.prepareStatement("select num,context from hive_wordcount")
    val rs = pstmt.executeQuery()
    while (rs.next()){
      println(rs.getInt("num") + "," + rs.getString("context"))
    }

    rs.close()
    pstmt.close()
  }

}

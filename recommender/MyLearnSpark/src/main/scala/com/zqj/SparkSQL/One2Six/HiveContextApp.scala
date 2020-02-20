package com.zqj.SparkSQL.One2Six

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object HiveContextApp {

  def main(args: Array[String]): Unit ={

    val conf = new SparkConf()
    conf.setAppName("HiveContextApp").setMaster("local")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    hiveContext.table("emp").show()

    sc.stop()
  }


}

package com.zqj.SparkSQL.One2Six.oneVersion

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SQLContextApp {

  def main(args: Array[String]): Unit ={
    val path = args(0)

    val conf = new SparkConf()
    conf.setAppName("SQLContextApp").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 路径就是HDFS的路径
    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()

    sc.stop()
  }


}

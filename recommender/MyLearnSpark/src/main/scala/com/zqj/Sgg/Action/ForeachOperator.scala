package com.zqj.Sgg.Action

import org.apache.spark.{SparkConf, SparkContext}

object ForeachOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("FilterOperator")
    conf.set("spark.default.parallelism", "3")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)

    val resultRDD = rdd.collect()
    resultRDD.foreach(x => println(x)) // 在Driver端执行，foreach是Scala的方法

    rdd.foreach(x => println(x)) // 在Execute端执行

  }

}

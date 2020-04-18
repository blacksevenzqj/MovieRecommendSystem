package com.zqj.Sgg.Action

import org.apache.spark.{SparkConf, SparkContext}

object TakeOrderedOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("FilterOperator")
    conf.set("spark.default.parallelism", "3")
    val sc = new SparkContext(conf)

    val list = List(2,5,4,6,8,3)
    val listRDD = sc.parallelize(list)

    val resultRDD = listRDD.takeOrdered(2)
    resultRDD.foreach(x => println(x))
  }

}

package com.zqj.Sgg.Action

import org.apache.spark.{SparkConf, SparkContext}

object TakeOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("FilterOperator")
    conf.set("spark.default.parallelism", "3")
    val sc = new SparkContext(conf)

    val list = List(("a", 1), ("a", 3), ("c", 3), ("d", 5))
    val listRDD = sc.parallelize(list)

    val resultRDD = listRDD.take(2)
    resultRDD.foreach(x => println(x))
  }


}

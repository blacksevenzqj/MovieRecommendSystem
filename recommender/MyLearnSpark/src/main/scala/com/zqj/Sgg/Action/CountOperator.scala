package com.zqj.Sgg.Action

import org.apache.spark.{SparkConf, SparkContext}

object CountOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("FilterOperator")
    conf.set("spark.default.parallelism", "3")
    val sc = new SparkContext(conf)

    val list = List(("a", 1), ("a", 3), ("c", 3), ("d", 5))
    val listRDD = sc.parallelize(list)

    // 通过func函数聚集RDD中的所有元素，先聚合分区内数据，再聚合分区间数据。
    val resultRDD = listRDD.count()
    println(resultRDD)
  }
}

package com.zqj.Sgg.Transformation

import org.apache.spark.{SparkConf, SparkContext}

object ZipOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("SortByKeyOperator")
    conf.set("spark.default.parallelism", "4")
    val sc = new SparkContext(conf)

    val names1 = Array("xuruyun1", "xuruyun2", "xuruyun3", "xuruyun4", "xuruyun5")
    val nameRDD1 = sc.parallelize(names1)

    val names2 = Array("zhangxueyou1", "zhangxueyou2", "zhangxueyou3", "zhangxueyou4", "zhangxueyou5")
    val nameRDD2 = sc.parallelize(names2)

    // zip：1、分区数相同；2、每个分区中元素个数相同。另：分区数不变。
    names1.zip(names2).foreach(x => println(x))
  }

}

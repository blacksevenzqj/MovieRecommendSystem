package com.zqj.Sgg.Transformation

import org.apache.spark.{SparkConf, SparkContext}

object CartesianOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("SortByKeyOperator")
    conf.set("spark.default.parallelism", "4")
    val sc = new SparkContext(conf)

    val names1 = Array("xuruyun1", "xuruyun2", "xuruyun3", "xuruyun4", "xuruyun5")
    val nameRDD1 = sc.parallelize(names1)

    val names2 = Array("zhangxueyou1", "zhangxueyou2", "zhangxueyou3", "zhangxueyou4", "zhangxueyou5")
    val nameRDD2 = sc.parallelize(names2)

    // 笛卡尔积：分区数累加
    nameRDD1.cartesian(nameRDD2).foreach(x => println(x))

  }

}

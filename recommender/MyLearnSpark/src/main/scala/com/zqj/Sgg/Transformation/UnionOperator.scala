package com.zqj.Sgg.Transformation

import org.apache.spark.{SparkConf, SparkContext}

object UnionOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("SortByKeyOperator")
    conf.set("spark.default.parallelism", "4")
    val sc = new SparkContext(conf)

    val names1 = Array("xuruyun", "liangyongqi", "wangfeng", "zhangxueyou", "liudehua")
    val nameRDD1 = sc.parallelize(names1)

    val names2 = Array("xuruyun", "xuruyun2", "liangyongqi2", "wangfeng2", "zhangxueyou2", "liudehua2")
    val nameRDD2 = sc.parallelize(names2)
    // 并集：分区数累加
    nameRDD1.union(nameRDD2).foreach(x => println(x))

  }

}

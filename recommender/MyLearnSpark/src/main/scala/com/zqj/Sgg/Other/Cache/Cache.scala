package com.zqj.Sgg.Other.Cache

import org.apache.spark.{SparkConf, SparkContext}

object Cache {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Array("atguigu"))
    val nocache = rdd.map(_.toString+System.currentTimeMillis)

    nocache.foreach(x => println(x))
    nocache.foreach(x => println(x))
    nocache.foreach(x => println(x))

    println("" * 30)

    val nocache2 = rdd.map(_.toString+System.currentTimeMillis)
    nocache2.cache()

    nocache2.foreach(x => println(x))
    nocache2.foreach(x => println(x))
    nocache2.foreach(x => println(x))
    println(nocache2.toDebugString)
    println(nocache2.dependencies)

    sc.stop()

  }


}

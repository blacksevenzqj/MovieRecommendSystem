package com.zqj.Sxt.oneVersion.operator.Transformation

import org.apache.spark.{SparkConf, SparkContext}

object MapOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("MapOperator")
    val sc = new SparkContext(conf)
    map(sc)
    sc.stop()
  }

  def map(sc : SparkContext): Unit ={
    val list = List("张无忌", "赵敏", "周芷若")
    val listRDD = sc.parallelize(list)
    val nameRDD = listRDD.map(name => "Hello " + name)
    nameRDD.foreach(name => println(name))
  }

}

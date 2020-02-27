package com.zqj.Sxt.oneVersion.operator.Transformation

import org.apache.spark.{SparkConf, SparkContext}

object FlatMapOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("FlatMapOperator")
    val sc = new SparkContext(conf)
    flatMap(sc)
    sc.stop()
  }

  def flatMap(sc : SparkContext): Unit ={
    val list = List("张无忌", "赵敏", "周芷若")
    val listRDD = sc.parallelize(list)
    val nameRDD = listRDD.flatMap(line => line.split(" ")).map(name => "Hello " + name)
    nameRDD.foreach(name => println(name))
  }

}

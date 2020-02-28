package com.zqj.Sxt.oneVersion.operator.Transformation

import org.apache.spark.{SparkConf, SparkContext}

object FilterOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("FilterOperator")
    val sc = new SparkContext(conf)
    Filter(sc)
    sc.stop()
  }

  def Filter(sc : SparkContext): Unit ={
    val list = List("张无忌", "赵敏", "周芷若")
    val listRDD = sc.parallelize(list)
    val nameRDD = listRDD.filter(name => name.startsWith("张"))
    nameRDD.foreach(name => println(name))
  }


}

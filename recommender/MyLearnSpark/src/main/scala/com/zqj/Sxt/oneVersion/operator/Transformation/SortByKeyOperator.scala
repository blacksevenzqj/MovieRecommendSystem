package com.zqj.Sxt.oneVersion.operator.Transformation

import org.apache.spark.{SparkConf, SparkContext}

object SortByKeyOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("SortByKeyOperator")
    val sc = new SparkContext(conf)
    sortByKey(sc)
    sc.stop()
  }

  def sortByKey(sc : SparkContext): Unit ={
    val list = List((99, "张三丰"), (96, "东方不败"), (66, "林平之"), (98, "聂风"))
    sc.parallelize(list).sortByKey(false).foreach(tuple => println(tuple._2 + "->" + tuple._1))
  }
}

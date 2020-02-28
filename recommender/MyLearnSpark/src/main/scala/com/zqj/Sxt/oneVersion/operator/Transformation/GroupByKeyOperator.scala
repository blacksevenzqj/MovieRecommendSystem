package com.zqj.Sxt.oneVersion.operator.Transformation

import org.apache.spark.{SparkConf, SparkContext}

object GroupByKeyOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("GroupByKeyOperator")
    val sc = new SparkContext(conf)
    groupByKey(sc)
    groupByKey2(sc)
    sc.stop()
  }

  def groupByKey(sc : SparkContext): Unit = {
    val list = List(("武当", "张三丰"), ("峨眉", "灭绝师太"), ("武当", "宋青书"), ("峨眉", "周芷若"))
    val listRDD = sc.parallelize(list)
    val nameRDD = listRDD.groupByKey()
    nameRDD.foreach(t => {
      val menpai = t._1
      val iterator = t._2.iterator
      var people = ""
      while (iterator.hasNext) people = people + iterator.next + " "
      println("门派:" + menpai + "人员:" + people)
    })
  }

  def groupByKey2(sc : SparkContext): Unit = {
    val list = List("hello spark", "hello world", "hello world")
    val listRDD = sc.parallelize(list)
    val mapRDD = listRDD.flatMap(line => line.split(" ")).map(name => (name, 1))
    val groupByRDD = mapRDD.groupByKey()

    groupByRDD.map(t => new Tuple2(t._1, t._2.toList)).foreach(t => print(t._1, t._2))
  }
}

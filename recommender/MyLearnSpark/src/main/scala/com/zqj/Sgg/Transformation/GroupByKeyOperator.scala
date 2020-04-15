package com.zqj.Sgg.Transformation

import org.apache.spark.{SparkConf, SparkContext}

// 按照key进行分组，直接进行shuffle
object GroupByKeyOperator {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[1]").setAppName("FilterOperator")
    conf.set("spark.default.parallelism", "3")
    val sc = new SparkContext(conf)

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

    println("-"*30)

    nameRDD.foreachPartition(iterator => {
      while(iterator.hasNext){
        val temp = iterator.next()
        val menpai = temp._1
        val iterator2 = temp._2.iterator
        var people = ""
        while (iterator2.hasNext) people = people + iterator2.next + " "
        println("门派:" + menpai + "人员:" + people)
      }
    })

  }

}

package com.zqj.Sgg.Transformation

import org.apache.spark.{SparkConf, SparkContext}

object FilterOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("FilterOperator")
    val sc = new SparkContext(conf)

    val list = List("张无忌", "赵敏", "周芷若", "张学友")
    val listRDD = sc.parallelize(list, 3)
    val nameRDD = listRDD.filter(name => name.startsWith("张"))

    nameRDD.foreachPartition(iterator => {
      while(iterator.hasNext){
        println(iterator.next())
      }
    })

    sc.stop()
  }

}

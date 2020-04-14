package com.zqj.Sgg.Transformation

import org.apache.spark.{SparkConf, SparkContext}

object SampleOperator {

   def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("FilterOperator")
    val sc = new SparkContext(conf)

    val list = List("张无忌", "赵敏", "周芷若", "张学友")
    val listRDD = sc.parallelize(list, 3)
     // 第一个参数：false非放回采样，true放回采样；
     val nameRDD = listRDD.sample(false, 0.33, 1L)

    nameRDD.foreachPartition(iterator => {
      while(iterator.hasNext){
        println(iterator.next())
      }
    })

    sc.stop()
  }


}

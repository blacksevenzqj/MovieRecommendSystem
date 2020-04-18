package com.zqj.Sgg.Transformation.k_v

import org.apache.spark.{SparkConf, SparkContext}

// 按照key进行聚合，在shuffle之前有combine（预聚合）操作（先在 各分区中进行聚合），返回结果是RDD[k,v]。
object ReduceByKeyOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("FilterOperator")
    conf.set("spark.default.parallelism", "3")
    val sc = new SparkContext(conf)

    val list = List("武当", "峨眉", "武当", "峨眉")
    val listRDD = sc.parallelize(list)
    val pairRDD = listRDD.map(x => (x, 1))
    val reduceRDD = pairRDD.reduceByKey((a, b) => a + b)
    reduceRDD.foreachPartition(iterator => {
      while(iterator.hasNext){
        val value = iterator.next()
        println(value)
      }
    })
  }

}

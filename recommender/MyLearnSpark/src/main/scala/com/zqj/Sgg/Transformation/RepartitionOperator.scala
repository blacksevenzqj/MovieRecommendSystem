package com.zqj.Sgg.Transformation

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object RepartitionOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("FilterOperator")
    val sc = new SparkContext(conf)

    val list = List("xuruyun1", "xuruyun2", "xuruyun3", "xuruyun4", "xuruyun5",
      "xuruyun7", "xuruyun8", "xuruyun9", "xuruyun10", "xuruyun11", "xuruyun12")
    val listRDD = sc.parallelize(list, 3)
    val rddMap = listRDD.mapPartitionsWithIndex((index, iterator) => {
      //      iterator.map(x => {(index, x)})  // 或

      val newList: ListBuffer[String] = ListBuffer()
      while(iterator.hasNext){
        val temp = iterator.next()
        newList.append(index + "_" + temp)
      }
      newList.toIterator
    })

    rddMap.foreach(name => println(name))

    println("-" * 30)

    val rddMap2 = rddMap.repartition(6) // 最终是调用了coalesce：coalesce(numPartitions, shuffle = true)
    val rddMap3 = rddMap2.mapPartitionsWithIndex((index, iterator) => {
      //      iterator.map(x => {(index, x)})  // 或

      val newList: ListBuffer[String] = ListBuffer()
      while(iterator.hasNext){
        val temp = iterator.next()
        newList.append(index + "_" + temp)
      }
      newList.toIterator
    })

    rddMap3.foreach(name => println(name))


    sc.stop()
  }

}

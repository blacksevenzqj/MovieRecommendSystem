package com.zqj.Sgg.Transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object MapPartitionsWithIndexOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("MapPartitionsOperator")
    conf.set("spark.default.parallelism", "3")
    val sc = new SparkContext(conf)

    def localCpuCount:Int = Runtime.getRuntime.availableProcessors()
    println(localCpuCount)  // 4

    val array = Array(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)
    val rdd: RDD[Int] = sc.parallelize(array) // sc.parallelize(array, 10)
    val rddMap = rdd.mapPartitionsWithIndex((index, iterator) => {
//      iterator.map(x => {(index, x)})  // 或

      val newList: ListBuffer[String] = ListBuffer()
      while(iterator.hasNext){
        val temp = iterator.next()
        newList.append(index + "_" + temp)
      }
      newList.toIterator
    })

    rddMap.foreach(name => println(name))


    sc.stop()
  }



}

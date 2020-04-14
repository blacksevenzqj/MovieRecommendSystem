package com.zqj.Sgg.Transformation

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


object MapPartitionsOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("MapPartitionsOperator")
    val sc = new SparkContext(conf)

    val array = Array(1,2,3,4,5,6,7,8,9)
    val rdd: RDD[Int] = sc.parallelize(array)
    val rddMap = rdd.mapPartitions(iterator => {
      val newList: ListBuffer[Int] = ListBuffer()
      while(iterator.hasNext){
        val num: Int = iterator.next()
        newList.append(num)
      }
      newList.toIterator
    })

    rddMap.foreach(name => println(name))

    sc.stop()
  }


}

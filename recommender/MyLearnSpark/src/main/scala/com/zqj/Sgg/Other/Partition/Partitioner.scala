package com.zqj.Sgg.Other.Partition

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.HashPartitioner

import scala.collection.mutable.ListBuffer

object Partitioner {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val pairs = sc.parallelize(List(("female",1),("male",5),("female",5),("male",2)), 2)
    println(pairs.partitioner)
    val rddMap = pairs.mapPartitionsWithIndex((index, iterator) => {
      //      iterator.map(x => {(index, x)})  // 或

      val newList: ListBuffer[String] = ListBuffer()
      while(iterator.hasNext){
        val temp = iterator.next()
        newList.append(index + "_" + temp)
      }
      newList.toIterator
    })

    rddMap.foreach(name => println(name))

    println("" * 30)

    val partitioned = pairs.partitionBy(new HashPartitioner(3))
    println(partitioned.partitioner)
    val rddMap2 = partitioned.mapPartitionsWithIndex((index, iterator) => {
      //      iterator.map(x => {(index, x)})  // 或

      val newList: ListBuffer[String] = ListBuffer()
      while(iterator.hasNext){
        val temp = iterator.next()
        newList.append(index + "_" + temp)
      }
      newList.toIterator
    })

    rddMap2.foreach(name => println(name))


    sc.stop()

  }

}

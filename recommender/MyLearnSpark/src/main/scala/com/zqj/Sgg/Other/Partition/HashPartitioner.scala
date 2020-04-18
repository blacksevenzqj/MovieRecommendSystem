package com.zqj.Sgg.Other.Partition

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object HashPartitioner {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val nopar = sc.parallelize(List((1,3),(1,2),(2,4),(2,3),(3,6),(3,8)),8)
    println(nopar.partitioner)
    val rddMap = nopar.mapPartitionsWithIndex((index, iterator) => {
      //      iterator.map(x => {(index, x)})  // 或

      val newList: ListBuffer[String] = ListBuffer()
      while(iterator.hasNext){
        val temp = iterator.next()
        newList.append(index + "_" + temp)
      }
      newList.toIterator
    })
    rddMap.foreach(name => println(name))

    val rddMap2 = nopar.mapPartitionsWithIndex((index,iter)=>{Iterator(index.toString+" : "+iter.mkString("|"))})
    rddMap2.foreach(name => println(name))


    println("" * 30)


    val hashpar = nopar.partitionBy(new org.apache.spark.HashPartitioner(7))
    println(hashpar.partitioner)
    println(hashpar.count)

    val rddMap3 = hashpar.mapPartitionsWithIndex((index, iterator) => {
      //      iterator.map(x => {(index, x)})  // 或

      val newList: ListBuffer[String] = ListBuffer()
      while(iterator.hasNext){
        val temp = iterator.next()
        newList.append(index + "_" + temp)
      }
      newList.toIterator
    })
    rddMap3.foreach(name => println(name))

    // 统计每个分区的数据量
    val shuzu = hashpar.mapPartitions(iter => Iterator(iter.length)).collect()
    shuzu.foreach(x => println(x))

    sc.stop()

  }

}

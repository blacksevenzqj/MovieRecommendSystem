package com.zqj.Sgg.Transformation.k_v

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object PartitionByOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("FilterOperator")
    conf.set("spark.default.parallelism", "3")
    val sc = new SparkContext(conf)

    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
    val listRDD = sc.parallelize(list, 5)

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

    println("-"*30)

    // 只有K、V类型才有分区器partitioner，才能使用partitionBy
    val pairRdd = listRDD.map((_,1))
    val pairRdd2 = pairRdd.partitionBy(new org.apache.spark.HashPartitioner(2)) // 使用Hash策略切分

    val rddMap2 = pairRdd2.mapPartitionsWithIndex((index, iterator) => {
      //      iterator.map(x => {(index, x)})  // 或

      val newList: ListBuffer[String] = ListBuffer()
      while(iterator.hasNext){
        val temp = iterator.next()
        newList.append(index + "_" + temp)
      }
      newList.toIterator
    })

    rddMap2.foreach(name => println(name))

  }

}

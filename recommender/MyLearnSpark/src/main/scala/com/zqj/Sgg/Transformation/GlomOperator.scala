package com.zqj.Sgg.Transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object GlomOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("MapPartitionsOperator")
    conf.set("spark.default.parallelism", "3")
    val sc = new SparkContext(conf)

    def localCpuCount:Int = Runtime.getRuntime.availableProcessors()
    println(localCpuCount)

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

    // glom()：将每个分区形成一个数组；collect()：将所有数据收集到一个数组中。
    // 两者结合：glom每个分区形成一个数组，collect在外层再套上一个数组：Array(Array(1,2,3), Array(4,5,6), ...)
    val glomv = rddMap.glom().collect()
    glomv.foreach(x => x.map(x => println(x)))

    sc.stop()
  }

}

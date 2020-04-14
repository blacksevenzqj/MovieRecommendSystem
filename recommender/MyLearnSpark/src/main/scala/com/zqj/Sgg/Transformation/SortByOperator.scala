package com.zqj.Sgg.Transformation

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object SortByOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("SortByKeyOperator")
    conf.set("spark.default.parallelism", "4")
    val sc = new SparkContext(conf)

    val scoreList = Array(Tuple2(65, "leo"), Tuple2(50, "tom"), Tuple2(100, "marry"), Tuple2(85, "jack"), Tuple2(66, "feiji"))
    val scores = sc.parallelize(scoreList)
    val rddMap = scores.mapPartitionsWithIndex((index, iterator) => {
      //      iterator.map(x => {(index, x)})  // 或

      val newList: ListBuffer[String] = ListBuffer()
      while(iterator.hasNext){
        val temp = iterator.next()
        newList.append(index + "_" + temp._1 + ":" + temp._2)
      }
      newList.toIterator
    })

    rddMap.foreach(name => println(name))

    println("-"*30)

    // 当设置的平行度>1时：sortBy会自动重分区
    val sortedScores = scores.sortBy(x => x._1, false)
    sortedScores.foreach(name => println(name))

    println("-"*30)

    // 分区打印：
    val rddMap2 = sortedScores.mapPartitionsWithIndex((index, iterator) => {
      //      iterator.map(x => {(index, x)})  // 或

      val newList: ListBuffer[String] = ListBuffer()
      while(iterator.hasNext){
        val temp = iterator.next()
        newList.append(index + "_" + temp._1 + ":" + temp._2)
      }
      newList.toIterator
    })

    rddMap2.foreach(name => println(name))

    sc.stop()
  }

}

package com.zqj.Sgg.Transformation

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object FoldByKeyOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("FilterOperator")
    conf.set("spark.default.parallelism", "3")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

    val rddMap = rdd.mapPartitionsWithIndex((index, iterator) => {
      //      iterator.map(x => {(index, x)})  // 或

      val newList: ListBuffer[String] = ListBuffer()
      while (iterator.hasNext) {
        val temp = iterator.next()
        newList.append(index + "_" + temp)
      }
      newList.toIterator
    })

    rddMap.foreach(name => println(name))

    // aggregateByKey的简化操作，seqop和combop相同
    val fold = rdd.foldByKey(0)(_+_)
    fold.foreach(x => println(x))

    val reduce = rdd.reduceByKey((a, b) => a + b)
    reduce.foreach(x => println(x))

    val agg = rdd.aggregateByKey(0)(_+_,_+_)
    agg.foreach(x => println(x))
  }

}

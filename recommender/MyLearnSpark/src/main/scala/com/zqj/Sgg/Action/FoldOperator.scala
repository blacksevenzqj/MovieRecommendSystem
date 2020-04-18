package com.zqj.Sgg.Action

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object FoldOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("FilterOperator")
    conf.set("spark.default.parallelism", "3")
    val sc = new SparkContext(conf)

    var rdd1 = sc.makeRDD(1 to 10,2)
    val rddMap = rdd1.mapPartitionsWithIndex((index, iterator) => {
      //      iterator.map(x => {(index, x)})  // 或

      val newList: ListBuffer[String] = ListBuffer()
      while(iterator.hasNext){
        val temp = iterator.next()
        newList.append(index + "_" + temp)
      }
      newList.toIterator
    })

    rddMap.foreach(name => println(name))

    // 折叠操作，aggregate的简化操作，seqop和combop一样。
    println(rdd1.fold(0)(_+_))

    // 注意：aggregate进行分区间 累加操作时，还要加一个初始值10。而aggregateByKey不会。
    // Fold操作和aggregate相同，FoldByKey和aggregateByKey相同
    println(rdd1.fold(10)(_+_))

  }


}

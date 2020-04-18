package com.zqj.Sgg.Action

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object AggregateOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("FilterOperator")
    conf.set("spark.default.parallelism", "3")
    val sc = new SparkContext(conf)

    /**
      1. 参数：(zeroValue: U)(seqOp: (U, T) ⇒ U, combOp: (U, U) ⇒ U)
      2. 作用：aggregate函数将每个分区里面的元素通过seqOp和初始值进行聚合，然后用combine函数将每个分区的结果和初始值(zeroValue)进行combine操作。
         这个函数最终返回的类型不需要和RDD中元素类型一致。
      */
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

    println(rdd1.aggregate(0)(_+_,_+_))

    // 注意：aggregate进行 分区间 累加操作时，还要加一个初始值10。而aggregateByKey不会。
    println(rdd1.aggregate(10)(_+_,_+_))

  }

}

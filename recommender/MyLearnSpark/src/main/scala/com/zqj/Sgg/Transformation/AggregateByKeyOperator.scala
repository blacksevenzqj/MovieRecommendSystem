package com.zqj.Sgg.Transformation

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

// https://blog.csdn.net/qq_35440040/article/details/82691794
object AggregateByKeyOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("FilterOperator")
    conf.set("spark.default.parallelism", "3")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)

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

    println("-"*30)

    /**
      （1）zeroValue：给每一个分区中的每一个key一个初始值；
      （2）seqOp：函数用于在每一个分区中用初始值逐步迭代value；
      （3）combOp：函数用于合并每个分区中的结果。
      */
    // 第一个参数：初始值
    // 第二个参数（函数）：在Map阶段 取出每个分区相同key对应值的最大值
    // 第三个参数（函数）：Reduce阶段 相加
    val agg = rdd.aggregateByKey(0)(math.max(_,_),_+_)

    val rddMap2 = agg.mapPartitionsWithIndex((index, iterator) => {
      //      iterator.map(x => {(index, x)})  // 或

      val newList: ListBuffer[String] = ListBuffer()
      while(iterator.hasNext){
        val temp = iterator.next()
        newList.append(index + "_" + temp)
      }
      newList.toIterator
    })

    rddMap2.foreach(name => println(name))

    agg.foreach(x => println(x))



  }

}

package com.zqj.Sgg.Transformation

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object CoalesceOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("FilterOperator")
    val sc = new SparkContext(conf)

    val list = List("xuruyun1", "xuruyun2", "xuruyun3", "xuruyun4", "xuruyun5",
      "xuruyun7", "xuruyun8", "xuruyun9", "xuruyun10", "xuruyun11", "xuruyun12")
    val listRDD = sc.parallelize(list, 6)
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

    println("-" * 30)

    /**
      * 1、压缩Partition：
      *RDD.coalesce(x)：默认shuffle=false时，只有Partition在一台机器上的时候才能生效，如果Partition分布在多台机器上不生效。
      * shuffle=true时，Partition所在多台机器进行shuffle代价大。
      * 2、扩展Partition：
      *RDD.coalesce(x, shuffle=True)：这时就相当于repartition了，因为repartition最终调用了coalesce(numPartitions, shuffle = true)
      **/
    val rddMap2 = rddMap.coalesce(3)
    val rddMap3 = rddMap2.mapPartitionsWithIndex((index, iterator) => {
      //      iterator.map(x => {(index, x)})  // 或

      val newList: ListBuffer[String] = ListBuffer()
      while(iterator.hasNext){
        val temp = iterator.next()
        newList.append(index + "_" + temp)
      }
      newList.toIterator
    })

    rddMap3.foreach(name => println(name))


    sc.stop()
  }


}

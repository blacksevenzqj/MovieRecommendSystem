package com.zqj.Sxt.oneVersion.operator.Transformation

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object MapPartitionsOperator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("MapPartitionsOperator")
    val sc = new SparkContext(conf)
    mapPartitions(sc)
    sc.stop()
  }

  def mapPartitions(sc : SparkContext): Unit ={
    val list = List(1,2,3,4,5,6)
    val listRDD = sc.parallelize(list,2)
    listRDD.mapPartitions(iterator => {
      val newList: ListBuffer[String] = ListBuffer()
      while (iterator.hasNext){
        newList.append("hello " + iterator.next())
      }
      newList.toIterator
    }).foreach(name => println(name))
  }

}

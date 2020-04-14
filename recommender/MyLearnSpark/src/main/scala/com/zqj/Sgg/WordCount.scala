package com.zqj.Sgg

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val text = sc.textFile("E:\\code\\python_workSpace\\idea_space\\lzy_spark_code\\src\\main\\pyspark\\First\\word.txt")

    val words = text.flatMap{line => line.split(" ")}
    val pairs = words.map(word => (word, 1))
    val result = pairs.reduceByKey((a, b) => a + b)
    val sorted = result.sortByKey(true)
    sorted.foreach(x => println(x))

    println()

    val reverse = result.map(tuple => (tuple._2, tuple._1))
    val reverseSort = reverse.sortByKey(true).map(tuple => (tuple._2, tuple._1))
    reverseSort.foreach(x => println(x))

    sc.stop()

  }


}

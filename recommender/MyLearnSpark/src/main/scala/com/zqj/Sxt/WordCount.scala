package com.zqj.Sxt

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]):Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("wc")
    val sc = new SparkContext(conf)

    val text = sc.textFile("file:///usr/local/my_soft/my_learn_code/my_learn_spark_code/MyLearnSpark/src/main/resources/data/test.txt")
//    val text = sc.textFile("E:\\code\\python_workSpace\\idea_space\\lzy_spark_code\\src\\main\\pyspark\\First\\word.txt")

    val words = text.flatMap{ line => line.split(" ") }
    val pairs = words.map( word => (word, 1))
//    val results = pairs.reduceByKey(_+_)
    val results = pairs.reduceByKey((a, b) => a + b)
    val sorted = results.sortByKey(true)
    sorted.foreach( x => println(x))

    println()

    val reverse = results.map(tuple => (tuple._2, tuple._1))
    val reverseSorted= reverse.sortByKey(false).map(tuple => (tuple._2, tuple._1))
    reverseSorted.foreach{ x => println(x) }

    sc.stop()
  }

}

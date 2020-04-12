package com.zqj.Sxt

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]):Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("wc")
    val sc: SparkContext = new SparkContext(conf)

//    val text: RDD[String] = sc.textFile("hdfs://hadoop104:9000/user/hadoop/word.txt") // HDFS
//    val text: RDD[String] = sc.textFile("file:///usr/local/my_soft/my_learn_code/my_learn_spark_code/MyLearnSpark/word.txt")
    val text = sc.textFile("E:\\code\\python_workSpace\\idea_space\\lzy_spark_code\\src\\main\\pyspark\\First\\word.txt")

    val words: RDD[String] = text.flatMap{ line => line.split(" ") }
    val pairs: RDD[(String, Int)] = words.map(word => (word, 1))
    val results: RDD[(String, Int)] = pairs.reduceByKey((a, b) => a + b) // pairs.reduceByKey(_+_)
    val sorted: RDD[(String, Int)] = results.sortByKey(true)
    sorted.foreach( x => println(x))

    println()

    val reverse = results.map(tuple => (tuple._2, tuple._1))
    val reverseSorted= reverse.sortByKey(false).map(tuple => (tuple._2, tuple._1))
    reverseSorted.foreach{ x => println(x) }

    sc.stop()
  }

}

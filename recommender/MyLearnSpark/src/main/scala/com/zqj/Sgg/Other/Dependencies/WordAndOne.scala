package com.zqj.Sgg.Other.Dependencies

import org.apache.spark.{SparkConf, SparkContext}

object WordAndOne {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val wordAndOne = sc.textFile("E:\\code\\python_workSpace\\idea_space\\lzy_spark_code\\src\\main\\pyspark\\First\\word.txt")

    val words = wordAndOne.flatMap{line => line.split(" ")}
    val pairs = words.map(word => (word, 1))
    val wordAndCount = pairs.reduceByKey((a, b) => a + b)

    // 查看“pairs”的Lineage
    println(pairs.toDebugString)
    // 查看“wordAndCount”的Lineage
    println(wordAndCount.toDebugString)

    println("-"*30)

    // 查看“pairs”的依赖类型
    println(pairs.dependencies) // OneToOneDependency 窄依赖
    // 查看“wordAndCount”的依赖类型
    println(wordAndCount.dependencies) // ShuffleDependency 宽依赖

    sc.stop()

  }


}

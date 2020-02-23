package com.zqj.Sxt.oneVersion.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

object StreamingWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")

    val ssc = new StreamingContext(conf, Seconds(1))

    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap { _.split(" ") }
    val pairs = words.map { word => (word, 1) }
    val wordCounts = pairs.reduceByKey(_+_)

    Thread.sleep(5000)
    wordCounts.print() // Action操作

    ssc.start()
    ssc.awaitTermination()
  }

}
package com.atguigu.recommender

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.{SparkContext, SparkConf}

object HelloWorld {
  def main(args: Array[String]):Unit={
    println("hello world")

    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    println(simpleDateFormat.format(new Date(1260759144 * 1000L)))
    println(String.valueOf(1260759144).length())

    val p=List(("hello",35,1.50),("nihao",36,1.78),("fly",39,3.50))
    println(p.map(_._1))
    println(p.map(_._2))
    println(p.map(_._3))
    // 遍历每个元组：一个元组的第2个数 > 其他元组的第二个数
    println(p.sortWith(_._2>_._2))

//    val conf = new SparkConf()
//    conf.setAppName("test")
//    conf.setMaster("local")
//    val sc = new SparkContext(conf)
//    val rdd = sc.parallelize(List((1, "a"), (1, "b"), (1, "c"), (2, "d"), (2, "e"), (3, "g")) )
//    val rddMap = rdd.collectAsMap()
//    val rddMap2 = rdd.collect().toMap
//    rddMap.foreach(println(_))
//    rddMap2.foreach(println(_))

    val as = List((1, "a"), (1, "b"), (1, "c"), (2, "d"), (2, "e"), (3, "g"))
    val bs = List((1, "a"), (1, "b"), (1, "c"), (2, "d"), (2, "e"), (3, "g"))
    val cs = List((1, "a"), (1, "b"), (1, "c"), (2, "d"), (2, "e"), (3, "g"))
    // 相当于 内外2层for循环（笛卡尔积）
    for( a <- as; b <- bs ; c <- cs){
      println(a, b, c)
    }

  }
}

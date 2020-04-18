package com.zqj.Sgg.Other.Function_transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SeriTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[1]").setAppName("FilterOperator")
        conf.set("spark.default.parallelism", "3")
        val sc = new SparkContext(conf)

        val rdd = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))

        //3.创建一个Search对象
        val search = new Search("h")

        // 错误方式1：在Execute执行时 调用 没有序列化的对象 的方法
//        val match1: RDD[String] = search.getMatch1(rdd)

        // 错误方式2：在Execute执行时 调用 匿名函数时，使用了 没有序列化对象 的参数
//        val match1: RDD[String] = search.getMatche2(rdd)
        val match1: RDD[String] = search.getMatche22(rdd)

        match1.collect().foreach(println)
    }
}


//class Search(query:String) extends Serializable { // 错误方式1/2 的 共同解决方式：对象可序列化
class Search(query:String){
    //过滤出包含字符串的数据
    def isMatch(s: String): Boolean = {
        s.contains(query)
    }
    //过滤出包含字符串的RDD
    def getMatch1(rdd: RDD[String]): RDD[String] = {
        rdd.filter(isMatch) // filter中运行的 isMatch方法 是对象方法
    }

    //过滤出包含字符串的RDD
    def getMatche2(rdd: RDD[String]):RDD[String] = {
        rdd.filter(x => x.contains(query)) // 匿名函数中使用了 对象参数
    }
    // 错误方式2 的 解决方式：将类变量赋值给局部变量
    def getMatche22(rdd: RDD[String]):RDD[String] = {
        val query_ : String = this.query // 将类变量赋值给局部变量
        rdd.filter(x => x.contains(query_)) // 匿名函数中使用了 对象参数
    }
}


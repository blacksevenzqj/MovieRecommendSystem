package com.zqj.Mk.SparkSQL.twoVersion

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object DataFrameRDDApp {

  def main(args: Array[String]): Unit ={
    val path = args(0)
    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local").getOrCreate()

    // 1、反射模式：通过反射的方式RDD与DataFrame转换（需提前知道字段类型）
    inferReflection(spark, path)

    println("---------------------------------------------------")

    // 2、参数模式：提前不知道字段数据类型
    program(spark, path)

    spark.stop()
  }

  // 1、通过反射的方式RDD与DataFrame转换（需提前知道字段类型）
  def inferReflection(spark: SparkSession, path: String): Unit ={
    // 路径就是HDFS的路径
    val rdd = spark.sparkContext.textFile(path)

    // 需要导入隐式转换：
    import spark.implicits._
    val infoDF = rdd.map(_.split(",")).map(line => Info(line(0).toInt, line(1), line(2).toInt)).toDF()
    infoDF.show()
    infoDF.filter(infoDF.col("age")>29).show()

    infoDF.createOrReplaceTempView("infos")
    spark.sql("select * from infos where age > 30").show()
  }

  case class Info(id: Int, name: String, age:Int)


  // 2、参数模式：提前不知道字段数据类型
  def program(spark: SparkSession, path: String): Unit ={
    // 路径就是HDFS的路径
    val rdd = spark.sparkContext.textFile(path)
    val infoRDD = rdd.map(_.split(",")).map(line => Row(line(0).toInt, line(1), line(2).toInt))
    val structType = StructType(Array(StructField("id",IntegerType, true),
      StructField("name",StringType, true),StructField("age",IntegerType, true)
    ))

    val infoDF = spark.createDataFrame(infoRDD, structType)
    infoDF.show()

    infoDF.filter(infoDF.col("age")>29).show()

    infoDF.createOrReplaceTempView("infos")
    spark.sql("select * from infos where age > 30").show()

  }


}

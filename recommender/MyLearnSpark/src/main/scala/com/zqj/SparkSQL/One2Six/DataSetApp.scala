package com.zqj.SparkSQL

import org.apache.spark.sql.SparkSession

object DataSetApp {

  def main(args: Array[String]): Unit ={
    val path = args(0)
    val spark = SparkSession.builder().appName("DataSetApp").master("local").getOrCreate()

    // 需要导入隐式转换：
    import spark.implicits._

    val df = spark.read.option("header", true).option("inferSchema", true).csv(path)
    df.show()
    df.map(line => line.getAs[Int]("PassengerId")).show()
    df.select(df.col("PassengerId"), df("Name")).show()

    println("------------------------------------------------")

    val ds = df.as[Train]
    ds.map(line => line.PassengerId).show()
    ds.select(ds.col("PassengerId"), ds("Name")).show()


    spark.stop()
  }

  case class Train(PassengerId:Option[Int], Name:String, Sex:String, Age:Option[Int])


}

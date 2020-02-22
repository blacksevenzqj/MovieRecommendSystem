package com.zqj.Mk.SparkSQL.twoVersion

import org.apache.spark.sql.SparkSession

/*
spark-submit \
--class com.zqj.SparkSQL.HiveApp \
--name HiveApp \
--master local \
/usr/local/my_soft/my_learn_code/my_learn_spark_code/MyLearnSpark/target/MyLearnSpark-1.0-SNAPSHOT.jar
 */
object HiveApp {

  def main(args: Array[String]): Unit ={

    // 路径就是HDFS的路径
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    spark.sql("show databases").show()
    spark.sql("use sparklearn")
    spark.sql("select * from hive_wordcount").show()


    spark.stop()
  }

}

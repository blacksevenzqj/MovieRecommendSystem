package com.zqj.Mk.SparkSQL.twoVersion

import org.apache.spark.sql.SparkSession

/*
spark-submit \
--class com.zqj.SparkSQL.SparkSessionApp \
--master local \
/usr/local/my_soft/my_learn_code/my_learn_spark_code/MyLearnSpark/target/MyLearnSpark-1.0-SNAPSHOT.jar \
/user/hadoop/people.json
 */
object SparkSessionApp {

  def main(args: Array[String]): Unit ={
    val path = args(0)

    // 路径就是HDFS的路径
    val spark = SparkSession.builder().appName("SparkSessionApp").master("local").getOrCreate()
    val people = spark.read.json(path)

    people.show()
    people.select("name").show()
    people.select(people.col("name"), (people.col("age") + 10).as("age2")).show()
    people.filter(people.col("age") > 19).show()
    people.groupBy("age").count().show()

    spark.stop()
  }


}

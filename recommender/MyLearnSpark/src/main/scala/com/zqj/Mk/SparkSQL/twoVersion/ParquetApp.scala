package com.zqj.Mk.SparkSQL.twoVersion

import org.apache.spark.sql.SparkSession

/*
spark-submit \
--class com.zqj.SparkSQL.ParquetApp \
--master local \
/usr/local/my_soft/my_learn_code/my_learn_spark_code/MyLearnSpark/target/MyLearnSpark-1.0-SNAPSHOT.jar \
/user/hadoop/users.parquet
 */
object ParquetApp {

  def main(args: Array[String]): Unit ={
    val path = args(0)
    val spark = SparkSession.builder().appName("DataSetApp").master("local").getOrCreate()

    val userDF = spark.read.format("parquet").load(path)

    userDF.printSchema()
    userDF.show()
    userDF.select("name", "favorite_color").show()
    // 以 HDFS文件格式 输出，文件内容是 JSON格式。
    userDF.select("name", "favorite_color").write.format("json").save("file:///usr/local/my_soft/spark-2.1.0-bin-h27hive/examples/src/main/resources/jsonout")

    // 不指定数据格式format，直接导入（默认数据格式为parquet）
    val userDF2 = spark.read.load(path)
    userDF2.show()


    // 使用SQL的方式访问：
    userDF2.createOrReplaceTempView("parquetTable")
    spark.sql("select * from parquetTable where name = 'Alyssa'").show()

    // 使用option指定路径读取：
    spark.read.format("parquet").option("path", path).load().show()

    spark.stop()
  }

}

package com.zqj.Mk.SparkSQL.twoVersion

import org.apache.spark.sql.SparkSession

object MySqlApp {

   def main(args: Array[String]): Unit ={

     // 路径就是HDFS的路径
     val spark = SparkSession.builder().appName("SparkSessionApp").master("local").getOrCreate()

     val jdbcDF = spark.read
       .format("jdbc")
       .option("url", "jdbc:mysql://hadoop104:3306/metastore")
       .option("dbtable", "metastore.TBLS")
       .option("user", "hadoop")
       .option("password", "1")
       .option("driver", "com.mysql.jdbc.Driver")
       .load()

     jdbcDF.show()


     jdbcDF.createOrReplaceTempView("tbls")
     spark.sql("select TBL_ID,DB_ID,TBL_NAME from tbls").show()


     spark.stop()
  }

}

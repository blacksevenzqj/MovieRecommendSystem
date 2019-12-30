package com.bfw.sparkMl.learn

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{Row, SparkSession}

object SparkML_example1 {

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]"
    )
    // 创建一个spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("My_First")
    // 创建spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    this.firstCase(spark)
  }

  def firstCase(spark: SparkSession): Unit ={
    val training = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(0.0,1.1,0.1)),
      (0.0, Vectors.dense(2.0,1.0,-1.0)),
      (0.0, Vectors.dense(2.0,1.3,1.0)),
      (1.0, Vectors.dense(0.0,1.2,-0.5))
    )).toDF("label","features")

    training.show(truncate = true)

    val lr = new LogisticRegression()
    println(lr.explainParams())
    lr.setMaxIter(10).setRegParam(0.01)

    val model1 = lr.fit(training)
    println(model1.parent.extractParamMap)


    val paramMap = ParamMap(lr.maxIter -> 20)
      .put(lr.maxIter,30) //会覆盖
      .put(lr.regParam -> 0.1, lr.threshold -> 0.55)

    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")
    val paramMapCombined = paramMap ++ paramMap2

    val model2 = lr.fit(training, paramMapCombined)
    println(model2.parent.extractParamMap)

    // 测试集：
    val test = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(-1.0,1.5,1.3)),
      (0.0, Vectors.dense(3.0,2.0,-0.1)),
      (1.0, Vectors.dense(0.0,2.2,-1.5))
    )).toDF("label","features")

    model1.transform(test).select("label","features","probability","prediction").collect()
        .foreach{case Row(label: Double, features: Vector, probability: Vector, prediction: Double) => println(s"($features, $label) -> probability=$probability, prediction=$prediction")}

  }

}

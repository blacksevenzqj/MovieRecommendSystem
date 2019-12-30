package com.bfw.sparkMl.learn

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{Row, SparkSession}


object SparkML_example3 {

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
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0),
      (4L, "b spark who", 1.0),
      (5L, "g d a y", 0.0),
      (6L, "spark fly", 1.0),
      (7L, "was mapreduce", 0.0),
      (8L, "e spark program", 1.0),
      (9L, "a e c l", 0.0),
      (10L, "spark compile", 1.0),
      (11L, "hadoop software", 0.0)
    )).toDF("id","text","label")

    training.show(truncate = true)

    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val wordsDataDF = tokenizer.transform(training)
    wordsDataDF.show(truncate = true)

    val hashingTF = new HashingTF().setInputCol(tokenizer.getOutputCol)
                                    .setOutputCol("features")
    hashingTF.transform(wordsDataDF).show(truncate = true)

    val lr = new LogisticRegression().setMaxIter(10)

    val pipeline = new Pipeline().setStages(Array(tokenizer,hashingTF,lr))

    val paramGrid = new ParamGridBuilder().
                          addGrid(hashingTF.numFeatures, Array(100,1000)). // 2次
                          addGrid(lr.regParam, Array(0.1,0.01)). // 2次
                          build()
    /*
    1、本方式：网格搜索（交叉验证）针对训练集相对较小时使用。
    2、CrossValidator意思 和 Sklearn的cross_val_score类似，但CrossValidator加入了ParamGridBuilder后 和 Sklearn的GridSearchCV类似。
    */
    val cv = new CrossValidator().
                    setEstimator(pipeline).
                    setEstimatorParamMaps(paramGrid).
                    setEvaluator(new BinaryClassificationEvaluator()).
                    setNumFolds(2) // 2次

    // 共运行：2 * 2 * 2 次
    val cvModel = cv.fit(training)
    println(cvModel.explainParams())

    val test = spark.createDataFrame(Seq(
      (12L, "spark h d e"),
      (13L, "a f c"),
      (14L, "mapreduce spark"),
      (15L, "apache hadoop")
    )).toDF("id","text")

    cvModel.transform(test).select("id","text","probability","prediction").collect()
         .foreach{case Row(id: Long, text: String, probability: Vector, prediction: Double) => println(s"($id, $text) -> probability=$probability, prediction=$prediction")}

  }

}

package com.bfw.sparkMl.learn

import org.apache.spark.SparkConf
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}



object SparkML_example2 {

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
      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id","text","label")

    training.show(truncate = true)

    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val wordsDataDF = tokenizer.transform(training)
    println(wordsDataDF)

    val hashingTF = new HashingTF().setNumFeatures(1000)
                                    .setInputCol(tokenizer.getOutputCol)
                                    .setOutputCol("features")
    println(hashingTF.transform(wordsDataDF))

    val lr = new LogisticRegression().
                  setMaxIter(10).setRegParam(0.01)

    val pipeline = new Pipeline().setStages(Array(tokenizer,hashingTF,lr))

    val model = pipeline.fit(training)
    pipeline.save("E:\\code\\workSpace\\mylearnObject\\git_directory\\SXT\\ECommerceRecommendSystem\\recommender\\OfflineRecommender\\src\\main\\scala\\com\\bfw\\sparkMl\\learn\\tmp\\sparkML-LRpipeline")
    model.save("E:\\code\\workSpace\\mylearnObject\\git_directory\\SXT\\ECommerceRecommendSystem\\recommender\\OfflineRecommender\\src\\main\\scala\\com\\bfw\\sparkMl\\learn\\tmp\\sparkML-LRmodel")

    val model2 = PipelineModel.load("E:\\code\\workSpace\\mylearnObject\\git_directory\\SXT\\ECommerceRecommendSystem\\recommender\\OfflineRecommender\\src\\main\\scala\\com\\bfw\\sparkMl\\learn\\tmp\\sparkML-LRmodel")

    val test = spark.createDataFrame(Seq(
        (4L, "spark h d e"),
        (5L, "a f c"),
        (6L, "mapreduce spark"),
        (7L, "apache hadoop")
    )).toDF("id","text")

    model2.transform(test).select("id","text","probability","prediction").collect()
         .foreach{case Row(id: Long, text: String, probability: Vector, prediction: Double) => println(s"($id, $text) -> probability=$probability, prediction=$prediction")}

  }

}

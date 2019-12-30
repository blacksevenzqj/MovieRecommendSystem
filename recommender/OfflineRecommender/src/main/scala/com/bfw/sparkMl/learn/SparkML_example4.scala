package com.bfw.sparkMl.learn

import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.{RegressionEvaluator}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.{SparkSession}


object SparkML_example4 {

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]"
    )
    // 创建一个spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("My_First")
//    val sc = new SparkContext(sparkConf)
    // 创建spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    this.firstCase(spark)
  }

  def firstCase(spark: SparkSession): Unit ={
    // 原文档中每行最前面有个+号，使用：sed -i 's/+//g' sample_linear_regression_data.txt 命令消除。
    val data = spark.read.format("libsvm").load("E:\\code\\workSpace\\mylearnObject\\git_directory\\SXT\\ECommerceRecommendSystem\\recommender\\OfflineRecommender\\src\\main\\scala\\com\\bfw\\sparkMl\\learn\\sample_linear_regression_data.txt")

    // data.randomSplit 相当于 Sklearn的train_test_split的数据切分
    val Array(training, test) = data.randomSplit(Array(0.9, 0.1), seed=12345)
    println(data.count(), training.count(), test.count())

    val lr = new LinearRegression()

    val paramGrid = new ParamGridBuilder().
                      addGrid(lr.elasticNetParam, Array(0.0,0.5,1.0)). // 3次
                      addGrid(lr.fitIntercept). // 1次（True/False是否允许截断，默认True）
                      addGrid(lr.regParam, Array(0.1,0.01)). // 2次
                      build()

    /*
    1、本方式：网格搜索 针对训练集较大时使用。
    2、TrainValidationSplit意思 和 Sklearn的train_test_split类似，但TrainValidationSplit进行数据切分之后，再进行网格搜索，只运行 网格搜索 的次数（不进行交叉验证）
    3、上面data.randomSplit进行纯粹的数据切分为训练集和测试集；这里的TrainValidationSplit将训练集 再次进行数据切分为 训练集2（80%）和效验集（20%）进行模型训练，
    只运行 网格搜索 的次数（这里是6次，每次网格搜索使用相同的 训练集2 和 效验集）。
   */
    val trainValidationSplit = new TrainValidationSplit().
      setEstimator(lr).
      setEstimatorParamMaps(paramGrid).
      setEvaluator(new RegressionEvaluator).
      setTrainRatio(0.8)

    // 共运行：3 * 2 次
    val model = trainValidationSplit.fit(training)
    println(model.explainParams())
    model.transform(test).select("features","label","prediction").show()

  }

}

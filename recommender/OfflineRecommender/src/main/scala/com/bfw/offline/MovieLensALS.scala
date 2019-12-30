package com.bfw.offline

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/*
功能要求：
1、 找出最受欢迎的的 50 部电影， 随机选择 10 部让用户即时评分， 并给用户推荐 50 部电影
算法要求：
1、 通过ALS实现推荐模型
2、 调优模型参数， 通过RMSE指标评估并刷选出最优模型
3、 创建基准线， 确保最优模型高于基准线
 */

object MovieLensALS {

  //1. Define a rating elicitation function
  def elicitateRating(movies: Seq[(Int, String)])={
    val prompt="Please rate the following movie(1-5(best) or 0 if not seen: )"
    println(prompt)
    val ratings = movies.flatMap{ x =>
      var rating: Option[Rating] = None
      var valid = false
      while(!valid){
        println(x._2+" :")
        try{
          val r = scala.io.StdIn.readInt() // 控制台输入评分
          if (r>5 || r<0){
            println(prompt)
          } else {
            valid = true
            if (r>0){
              rating = Some(Rating(0, x._1, r)) // 控制台输入评分的用户ID默认设置为0
            }
          }
        } catch{
          case e:Exception => println(prompt)
        }
      }
      rating match {
        case Some(r) => Iterator(r) // ArrayBuffer(Rating(0,2762,3.0), Rating(0,1,3.0), Rating(0,541,4.0))
        case None => Iterator.empty // 如果输入0分，执行case None => Iterator.empty，元素不会加入到迭代集合中。
      }
    }
    if (ratings.isEmpty){
      sys.error("No ratings provided!")
    } else {
      ratings
    }
  }

  //2. Define a RMSE computation function
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating]) = {
    val prediction = model.predict(data.map(x => (x.user, x.product))) // 参数为：userId 和 productId
    // .values 分别拿到2组 ((x.user,x.product),x.rating)) 中的 x.rating，得到：ratingP，ratingT
    val predDataJoined = prediction.map(x => ((x.user,x.product),x.rating)).join(data.map(x => ((x.user,x.product),x.rating))).values
    new RegressionMetrics(predDataJoined).rootMeanSquaredError
  }

  //3. Main
  def main(args: Array[String]) {
    val config = Map(
      "spark.cores" -> "local[2]"
    )

    //3.1 Setup env
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

//    if (args.length !=1){
//      println("Usage: movieLensHomeDir")
//      sys.exit(1)
//    }

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender").set("spark.executor.memory","500m")
    val sc = new SparkContext(sparkConf)

    //3.2 Load ratings data and know your data
//    val movieLensHomeDir = args(0)
    val movieLensHomeDir = "E:\\code\\workSpace\\mylearnObject\\git_directory\\SXT\\MovieRecommendSystem\\recommender\\OfflineRecommender\\src\\main\\scala\\com\\bfw\\offline\\ml-1m"
    val ratings = sc.textFile(new File(movieLensHomeDir, "ratings.dat").toString).map {line =>
       val fields = line.split("::")
      //timestamp, user, product, rating：timestamp%10是为了后面切分数据集
      (fields(3).toLong%10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }
    println(ratings.getClass)

    val movies = sc.textFile(new File(movieLensHomeDir, "movies.dat").toString).map {line =>
      val fields = line.split("::")
      //movieId, movieName
      (fields(0).toInt, fields(1))
    }.collectAsMap()
    println(movies.getClass)

    val numRatings = ratings.count()
    val numUser = ratings.map(x => x._2.user).distinct().count()
    val numMovie = ratings.map(_._2.product).distinct().count()

    println("Got " + numRatings + " ratings from " + numUser + " users on " + numMovie + " movies.")

    //3.3 Elicitate personal rating
    // countByValue()相当于groupby(product).count()
    val topMovies = ratings.map(_._2.product).countByValue().toSeq.sortBy(-_._2).take(50).map(_._1) // sortBy(-_._2)中-号表示逆序排序
    val random = new Random(0)
    // 50 * 0.2 ≈ 10个元素
    val selectMovies = topMovies.filter(x => random.nextDouble() < 0.2).map(x => (x, movies(x))) // movieId, movieName
    println(selectMovies.length)
    val myRatings = elicitateRating(selectMovies)
    println(myRatings)
    val myRatingsRDD = sc.parallelize(myRatings, 1)

    //3.4 Split data into train(60%), validation(20%) and test(20%)
    val numPartitions = 10
    // x._1 为 timestamp%10：切分数据集
    val trainSet = ratings.filter(x => x._1<6).map(_._2).union(myRatingsRDD).repartition(numPartitions).persist() // 60%数据集
    val validationSet = ratings.filter(x => x._1>=6 && x._1<8).map(_._2).persist() // 20%数据集
    val testSet = ratings.filter(x => x._1>=8).map(_._2).persist() // 20%数据集

    val numTrain = trainSet.count()
    val numValidation = validationSet.count()
    val numTest = testSet.count()

    println("Training data: " + numTrain + " Validation data: " + numValidation + " Test data: " + numTest)

    //3.5 Train model and optimize model with validation set
    val numRanks = List(8, 12)
    val numIters = List(10, 20)
    val numLambdas = List(0.1, 10.0)
    var bestRmse = Double.MaxValue
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestRanks = -1
    var bestIters = 0
    var bestLambdas = -1.0
    for(rank <- numRanks; iter <- numIters; lambda <- numLambdas){ // 相当于3重循环：最右边的为内层循环
      val model = ALS.train(trainSet, rank, iter, lambda)
      val validationRmse = computeRmse(model, validationSet)
      println("RMSE(validation) = " + validationRmse + " with ranks=" + rank + ", iter=" + iter + ", Lambda=" + lambda)

      if (validationRmse < bestRmse) {
        bestModel = Some(model)
        bestRmse = validationRmse
        bestIters = iter
        bestLambdas = lambda
        bestRanks = rank
      }
    }

    //3.6 Evaluate model on test set
    val testRmse = computeRmse(bestModel.get, testSet)
    println("The best model was trained with rank="+bestRanks+", Iter="+bestIters+", Lambda="+bestLambdas+
      " and compute RMSE on test is "+testRmse)

    //3.7 Create a baseline and compare it with best model
    val meanRating = trainSet.union(validationSet).map(_.rating).mean()
    val bestlineRmse = new RegressionMetrics(testSet.map(x => (x.rating, meanRating))).rootMeanSquaredError
    val improvement = (bestlineRmse - testRmse) / bestlineRmse * 100
    println("The best model improves the baseline by "+"%1.2f".format(improvement)+"%.")

    //3.8 Make a personal recommendation
    val moviesId = myRatings.map(_.product)
    val candidates = sc.parallelize(movies.keys.filter(!moviesId.contains(_)).toSeq)
    // 给控制台输入评分的用户推荐电影：
    val recommendations = bestModel.get
                            .predict(candidates.map(x => (0, x))) // 控制台输入评分的用户ID默认设置为0
                            .sortBy(-_.rating)
                            .take(50)

    var i = 0
    println("Movies recommended for you:")
    recommendations.foreach{ line =>
      println("%2d".format(i)+" :" + movies(line.product))
      i += 1
    }

    sc.stop()
  }
}

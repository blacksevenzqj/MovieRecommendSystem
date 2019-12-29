import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.SparkSession

case class Movie(movieId:Int, title:String, genres:Seq[String])
case class User(userId:Int, gender:String, age:Int, occupation:Int, zip:String)

object ALSExample{

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[2]"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")
    val sc = new SparkContext(sparkConf)
    // 创建一个SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    //Ratings analyst
    val ratingText = sc.textFile("E:\\code\\workSpace\\mylearnObject\\git_directory\\SXT\\MovieRecommendSystem\\recommender\\OfflineRecommender\\src\\main\\scala\\com\\bfw\\offline\\ml-1m\\ratings.dat")
    ratingText.first()
    val ratingRDD = ratingText.map(parseRating).cache()
    println("Total number of ratings: " + ratingRDD.count())
    println("Total number of movies rated: " + ratingRDD.map(_.product).distinct().count())
    println("Total number of users who rated movies: " + ratingRDD.map(_.user).distinct().count())

    val movieRDD = sc.textFile("E:\\code\\workSpace\\mylearnObject\\git_directory\\SXT\\MovieRecommendSystem\\recommender\\OfflineRecommender\\src\\main\\scala\\com\\bfw\\offline\\ml-1m\\movies.dat")
                      .map(parseMovie).cache()

    val userRDD = sc.textFile("E:\\code\\workSpace\\mylearnObject\\git_directory\\SXT\\MovieRecommendSystem\\recommender\\OfflineRecommender\\src\\main\\scala\\com\\bfw\\offline\\ml-1m\\users.dat")
                      .map(parseUser).cache()

    //Create DataFrames
    val ratingDF = ratingRDD.toDF()
    val movieDF = movieRDD.toDF()
    val userDF = userRDD.toDF()
    ratingDF.printSchema()
    movieDF.printSchema()
    userDF.printSchema()
    ratingDF.createOrReplaceTempView("ratings")
    movieDF.createOrReplaceTempView("movies")
    userDF.createOrReplaceTempView("users")

    val result = spark.sql(
      """select title, rmax, rmin, ucnt
          from
          (select product, max(rating) as rmax, min(rating) as rmin, count(distinct user) as ucnt
            from ratings group by product) ratingsCNT
          join movies on product = movieId
          order by ucnt desc""")
    result.show()

    val mostActiveUser = spark.sql(
      """select user, count(*) as cnt from ratings group by user order by cnt desc limit 10""")
    mostActiveUser.show()

    val result2 = spark.sql(
      """select distinct title, rating
          from ratings join movies on movieId = product where user=4169 and rating > 4""")
    result2.show()


   //ALS
   val splits = ratingRDD.randomSplit(Array(0.8,0.2), 0L)
   val trainingSet = splits(0).cache()
   val testSet = splits(1).cache()
   trainingSet.count()
   testSet.count()
   val model = (new ALS().setRank(20).setIterations(10).run(trainingSet))

   val recomForTopUser = model.recommendProducts(4169,5)
   println(recomForTopUser)
   val movieTitle = movieRDD.map(array => (array.movieId,array.title)).collect().toMap
   println(movieTitle)
   val recomResult = recomForTopUser.map(rating=>(movieTitle(rating.product),rating.rating)).foreach(println)


    val testUserProduct = testSet.map{
      case Rating(user,product,rating) => (user,product)
    }
    println(testUserProduct)

    val testUserProductPredict = model.predict(testUserProduct)
    testUserProductPredict.take(10).mkString("\n")

    val testSetPair = testSet.map{
      case Rating(user,product,rating) => ((user,product),rating)
    }
    val predictionsPair = testUserProductPredict.map{
      case Rating(user,product,rating) => ((user,product),rating)
    }

    val joinTestPredict = testSetPair.join(predictionsPair)
    val mae = joinTestPredict.map{
      case ((user,product),(ratingT,ratingP)) =>
        val err = ratingT - ratingP
        Math.abs(err)
    }.mean()
    println(mae)
    val mse = joinTestPredict.map{
      case ((user,product),(ratingT,ratingP)) =>
        val err = ratingT - ratingP
        Math.pow(err, 2)
    }.mean()
    val rmse = Math.sqrt(mse)
    println(rmse)

    //FP: ratingT<=1, ratingP>=4
    val fp = joinTestPredict.filter{
     case ((user,product),(ratingT,ratingP)) =>
     (ratingT <=1 & ratingP >=4)
    }
    println(fp.count())

    import org.apache.spark.mllib.evaluation._
    val ratingTP = joinTestPredict.map{
      case ((user,product),(ratingT,ratingP)) =>
      (ratingP,ratingT)
    }

    val evalutor = new RegressionMetrics(ratingTP)
    println(evalutor.meanAbsoluteError)
    println(evalutor.rootMeanSquaredError)

    sc.stop()
    spark.stop()
  }

  //Define parse function
  def parseMovie(str: String): Movie = {
    val fields=str.split("::")
    assert(fields.size==3)
    Movie(fields(0).toInt, fields(1).toString, Seq(fields(2)))
  }
  def parseUser(str: String): User = {
    val fields=str.split("::")
    assert(fields.size==5)
    User(fields(0).toInt, fields(1).toString, fields(2).toInt, fields(3).toInt, fields(4).toString)
  }
  def parseRating(str: String): Rating = {
    val fields=str.split("::")
    assert(fields.size==4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
  }

}
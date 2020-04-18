package com.zqj.Sgg.Transformation

import org.apache.spark.{SparkConf, SparkContext}

// 每个城市广告点击Top3
object Case_Study {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("SortByKeyOperator")
    conf.set("spark.default.parallelism", "4")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("E:\\code\\workSpace\\mylearnObject\\git_directory\\SXT\\MovieRecommendSystem\\recommender\\MyLearnSpark\\src\\main\\scala\\com\\zqj\\Sgg\\Transformation\\agent.log")

    val provinceAdToOne = rdd.map(x => {
      val temp: Array[String] = x.split(" ") // 1索引：城市ID；4索引：广告ID
      ((temp(1), temp(4)), 1)
    })

    val reduceByKeyRDD = provinceAdToOne.reduceByKey((a, b) => a + b)

    val tempRDD = reduceByKeyRDD.map(x => (x._1._1, (x._1._2,x._2)))

    val groupByRDD = tempRDD.groupByKey()

    val Top3RDD = groupByRDD.mapValues( x => {
      x.toList.sortWith((x, y) => x._2 > y._2).take(3) // 每个城市的前3名
    })

    Top3RDD.foreach(x => println(x))

  }


}

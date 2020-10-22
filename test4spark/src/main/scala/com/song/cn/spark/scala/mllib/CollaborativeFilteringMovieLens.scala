package com.song.cn.spark.scala.mllib

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructType}

case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

object CollaborativeFilteringMovieLens {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("CollaborativeFilteringMovieLens")
      .enableHiveSupport()
      .getOrCreate()

    def parseRating(str: String): Rating = {
      val fields = str.split(",")
      assert(fields.size == 4)
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
    }

    /*val ratings = spark
      .read
      .textFile("input/movie/").map(parseRating).toDF()*/
    val schema = new StructType()
      .add("userId", IntegerType)
      .add("movieId", IntegerType)
      .add("rating", DoubleType)
      .add("timestamp", LongType)
    val ratings = spark
      .read
      .option("header", "true") // 第一行是否为表头名称
      .option("inferSchema", "true") // 是否自行推断 schema
      //.schema(schema) // 设置表头
      .csv("input/movie/")
    ratings.show(10)
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
    // 设置ALS算法参数
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
      .setColdStartStrategy("drop")
    val pipeline = new Pipeline().setStages(Array(als))
    val model = pipeline.fit(training)
    val predictions = model.transform(test)
    // 设置模型评价指标
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")
  }
}

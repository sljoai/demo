package com.song.cn.spark.scala.mllib

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Row, SparkSession}

object Word2VecTest {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\sljoai\\java\\src\\test\\hadoop-common\\hadoop-common-2.6.0-bin")
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Word2Vec")
      .getOrCreate()

    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")
    // 设置word2vec参数
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)
    result.collect().foreach {
      case Row(text: Seq[_], features: Vector) =>
        println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n")
    }
  }
}

package com.song.cn.spark.scala.mllib

import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object ChiSqSelector {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\sljoai\\java\\src\\test\\hadoop-common\\hadoop-common-2.6.0-bin")
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Word2Vec")
      .getOrCreate()

    val data = Seq(
      (7,Vectors.dense(0.0,0.0,18.0,1.0),1.0),
      (8,Vectors.dense(0.0,1.0,12.0,0.0),0.0),
      (9,Vectors.dense(1.0,0.0,15.0,0.1),0.0)
    )

    import spark.implicits._
    val df  = spark.createDataset(data).toDF("id","features","clicked")
    // 配置卡方检验参数
    val selector = new ChiSqSelector()
      .setNumTopFeatures(3)
      .setFeaturesCol("features")
      .setLabelCol("clicked")
      .setOutputCol("selectedFeatures")

    val result = selector.fit(df).transform(df)
    println(s"ChiSqSelector output with top ${selector.getNumTopFeatures} features selected")
    result.show()
  }

}

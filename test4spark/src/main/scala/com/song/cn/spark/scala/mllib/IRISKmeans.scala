package com.song.cn.spark.scala.mllib

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.{PCA, StandardScaler, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object IRISKmeans {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("IRISKmeans")
      .getOrCreate()

    // iris数据集数据结构
    val fields = Array("id", "SepalLength", "SepalWidth", "PetalLength", "PetalWidth")
    val fieldsType = fields.map(
      r => if (r == "id" || r == "Species") {
        StructField(r, StringType)
      }
      else {
        StructField(r, DoubleType)
      }
    )
    val schema = StructType(fieldsType)
    val featureCols = Array("SepalLength", "SepalWidth", "PetalLength", "PetalWidth")

    val data = spark
      .read
      .schema(schema)
      .option("header", false)
      .csv("input/iris/")
    val vectorAssembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      // 主成分个数，依旧是降维之后的维数
      .setK(2)
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)

    // 设置 Kmeans 参数
    val kmeans = new KMeans()
      .setK(3)
      .setSeed(System.currentTimeMillis())
      .setMaxIter(10)
    val pipeline = new Pipeline()
      .setStages(Array(vectorAssembler, pca, scaler, kmeans))
    val model = pipeline.fit(data)
    val predictions = model.transform(data)
    predictions.show()
    val evaluator = new ClusteringEvaluator()
    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")

  }
}

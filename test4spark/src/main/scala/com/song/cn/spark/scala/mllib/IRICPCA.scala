package com.song.cn.spark.scala.mllib

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{PCA, StandardScaler, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object IRICPCA {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\sljoai\\java\\src\\test\\hadoop-common\\hadoop-common-2.6.0-bin")

    val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("IRISPCA")
    .getOrCreate()

    // 数据结构为花萼长度、花萼宽度、花瓣长度、花瓣宽度
    val fields = Array("id","Species","SepalLength","SepalWidth","PetalLength","PetalWidth")
    val fieldsType = fields.map(
        r => if (r == "id"||r == "Species")
               {StructField(r, StringType)}
             else
                {StructField(r, DoubleType)}
    )
    val schema = StructType(fieldsType)
    val featureCols = Array("SepalLength","SepalWidth","PetalLength","PetalWidth")
    // 默认从demo的根目录下执行
    val data=spark.read.schema(schema).csv("./test4spark/input/iris")
    val vectorAssembler = new VectorAssembler()
    .setInputCols(featureCols)
    .setOutputCol("features")
    val vectorData=vectorAssembler.transform(data)

    // 特征标准化
    // TODO: 此处有问题
    val standardScaler = new StandardScaler()
    .setInputCol("features")
    .setOutputCol("scaledFeatures")
    .setWithMean(true)
    .setWithStd(false)
    .fit(vectorData)
    val pca = new PCA()
    .setInputCol("scaledFeatures")
    .setOutputCol("pcaFeatures")
    // 主成分个数，也就是降维后的维数
    .setK(2)
    val pipeline = new Pipeline()
    .setStages(Array(vectorAssembler,standardScaler,pca))
    val model = pipeline.fit(data)
    // 对特征进行PCA降维
    model.transform(data).select("Species", "pcaFeatures").show(100, false)

  }

}

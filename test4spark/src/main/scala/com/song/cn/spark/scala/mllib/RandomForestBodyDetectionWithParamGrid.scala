package com.song.cn.spark.scala.mllib

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable

/**
 * 超参数及交叉验证相结合使用
 */
object RandomForestBodyDetectionWithParamGrid {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("RandomForestBodyDetectionWithParamGrid")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    // 读取数据集
    val dataFiles = spark.read.textFile("test4spark/input/bodydetect")
    // .rdd是什么含义呢？ 将DataSet 转换为 rdd。那为啥要转换为rdd呢？
    val rawData = dataFiles.map(r => r.toString().split(" ")).rdd.map(row => {
      val list = mutable.ArrayBuffer[Any]()
      for (i <- row.toSeq) {
        list.append(i)
      }
      Row.fromSeq(list.map(v => if (v.toString.toUpperCase == "NAN") Double.NaN else v.toString.toDouble))
    })
    val schema = StructType(Array(
      StructField("timestamp", DoubleType), StructField("activityId", DoubleType), StructField("hr", DoubleType),
      StructField("hand_temp", DoubleType), StructField("hand_accel1X", DoubleType), StructField("hand_accel1Y", DoubleType),
      StructField("hand_accel1Z", DoubleType), StructField("hand_accel2X", DoubleType), StructField("hand_accel2Y", DoubleType),
      StructField("hand_accel2Z", DoubleType), StructField("hand_gyroX", DoubleType), StructField("hand_gyroY", DoubleType),
      StructField("hand_gyroZ", DoubleType), StructField("hand_magnetX", DoubleType), StructField("hand_magnetY", DoubleType),
      StructField("hand_magnetZ", DoubleType), StructField("hand_orientX", DoubleType), StructField("hand_orientY", DoubleType),
      StructField("hand_orientZ", DoubleType), StructField("hand_orientD", DoubleType), StructField("chest_temp", DoubleType),
      StructField("chest_accel1X", DoubleType), StructField("chest_accel1Y", DoubleType), StructField("chest_accel1Z", DoubleType),
      StructField("chest_accel2X", DoubleType), StructField("chest_accel2Y", DoubleType), StructField("chest_accel2Z", DoubleType),
      StructField("chest_gyroX", DoubleType), StructField("chest_gyroY", DoubleType), StructField("chest_gyroZ", DoubleType),
      StructField("chest_magnetX", DoubleType), StructField("chest_magnetY", DoubleType), StructField("chest_magnetZ", DoubleType),
      StructField("chest_orientX", DoubleType), StructField("chest_orientY", DoubleType), StructField("chest_orientZ", DoubleType),
      StructField("chest_orientD", DoubleType), StructField("ankle_temp", DoubleType), StructField("ankle_accel1X", DoubleType),
      StructField("ankle_accel1Y", DoubleType), StructField("ankle_accel1Z", DoubleType), StructField("ankle_accel2X", DoubleType),
      StructField("ankle_accel2Y", DoubleType), StructField("ankle_accel2Z", DoubleType), StructField("ankle_gyroX", DoubleType),
      StructField("ankle_gyroY", DoubleType), StructField("ankle_gyroZ", DoubleType), StructField("ankle_magnetX", DoubleType),
      StructField("ankle_magnetY", DoubleType), StructField("ankle_magnetZ", DoubleType), StructField("ankle_orientX", DoubleType),
      StructField("ankle_orientY", DoubleType), StructField("ankle_orientZ", DoubleType), StructField("ankle_orientD", DoubleType)))
    val df = spark.createDataFrame(rawData, schema)
    // 数据集的列名，sensor_name 表示某个传感器的某个指标数据，例如，手上的传感器的温度指标为 hand_temp
    val allColumnNames = Array(
      "timestamp", "activityId", "hr") ++ Array(
      "hand", "chest", "ankle").flatMap(sensor =>
      Array(
        "temp",
        "accel1X", "accel1Y", "accel1Z",
        "accel2X", "accel2Y", "accel2Z",
        "gyroX", "gyroY", "gyroZ",
        "magnetX", "magnetY", "magnetZ",
        "orientX", "orientY", "orientZ", "orientD").map(name => s"${sensor}_${name}")
    )
    // 数据集中不需要的列、时间戳和方位数据，分别表示手、胸部、踝关节上传感器的第一个方位指标
    val ignoredColumns = Array(0, 16, 17, 18, 19, 33, 34, 35, 36, 50, 51, 52, 53)
    val inputColNames = ignoredColumns.map(l => allColumnNames(l))
    val columnNames = allColumnNames.filter {
      !inputColNames.contains(_)
    }
    // 滤掉不需要的列，并填充缺失值
    val typeTransformer = new FillMissingValueTransformer().setInputCols(inputColNames)
    // 构造标签列
    val labelIndexer = new StringIndexer()
      .setInputCol("activityId")
      .setOutputCol("indexedLabel")
      .fit(df)
    // 构造特征列
    val vectorAssembler = new VectorAssembler()
      .setInputCols(columnNames)
      .setOutputCol("featureVector")
    // 配置分类器
    val rfClassifier = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("featureVector")
      .setFeatureSubsetStrategy("auto")
      .setNumTrees(350)
      .setMaxBins(30)
      .setMaxDepth(30)
      .setImpurity("entropy")
      .setCacheNodeIds(true)
    /*val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)*/
    //val Array(trainingData, testData) = df.randomSplit(Array(0.8, 0.2))
    val data = df.distinct()
    // 构建整个Pipeline
    val pipeline = new Pipeline().setStages(
      Array(typeTransformer,
        labelIndexer,
        vectorAssembler,
        rfClassifier))
    // 构造超参数
    val paramGrid = new ParamGridBuilder()
      .addGrid(rfClassifier.numTrees,3 ::5 :: 10 :: Nil)
      .addGrid(rfClassifier.featureSubsetStrategy,"auto" :: "all" :: Nil)
      .addGrid(rfClassifier.impurity,"gini" :: "entropy" :: Nil)
      .addGrid(rfClassifier.maxBins,2 :: 5 :: Nil)
      .addGrid(rfClassifier.maxDepth, 3 :: 5 :: Nil)
      .build()
    // 评估器
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    // 交叉验证器
    val crossValidator = new CrossValidator()
      .setEstimator(pipeline)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)
      .setEvaluator(evaluator)
    val crossValidatorModel = crossValidator.fit(data)
    // 得到最好的模型
    val bestModel = crossValidatorModel.bestModel
    val bestPipelineModel = crossValidatorModel.bestModel.asInstanceOf[PipelineModel]
    val stages = bestPipelineModel.stages
    // 得到最佳的模型超参数
    val rfClassifierStage = stages(stages.length-1).asInstanceOf[RandomForestClassificationModel]
    val numTress = rfClassifierStage.getNumTrees
    val featureSubsetStrategy = rfClassifierStage.getFeatureSubsetStrategy
    val impurity = rfClassifierStage.getImpurity
    val maxBins = rfClassifierStage.getMaxBins
    val maxDepth = rfClassifierStage.getMaxDepth
    println(s"numTress: $numTress, featureSubsetStrategy: $featureSubsetStrategy, impurity: $impurity, maxBins: $maxBins, maxDepth: $maxDepth ")
  }
}

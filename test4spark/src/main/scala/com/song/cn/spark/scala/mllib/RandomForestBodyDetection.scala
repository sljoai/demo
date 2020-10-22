package com.song.cn.spark.scala.mllib

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable

object RandomForestBodyDetection {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("RandomForestBodyDetection")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    // 读取数据集
    val dataFiles = spark.read.textFile("input/bodydetect")
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
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)
    val Array(trainingData, testData) = df.randomSplit(Array(0.8, 0.2))
    // 构建整个Pipeline
    val pipeline = new Pipeline().setStages(
      Array(typeTransformer,
        labelIndexer,
        vectorAssembler,
        rfClassifier,
        labelConverter))
    val model = pipeline.fit(trainingData)
    val predictionResultDF = model.transform(testData)
    // 展示结果
    predictionResultDF.select(
      "hr", "hand_temp", "hand_accel1X", "hand_accel1Y", "hand_accel1Z", "hand_accel2X", "hand_accel2Y", "hand_accel2Z", "hand_gyroX", "hand_gyroY", "hand_gyroZ", "hand_magnetX", "hand_magnetY", "hand_magnetZ", "chest_temp", "chest_accel1X", "chest_accel1Y", "chest_accel1Z", "chest_accel2X", "chest_accel2Y", "chest_accel2Z", "chest_gyroX", "chest_gyroY", "chest_gyroZ", "chest_magnetX", "chest_magnetY", "chest_magnetZ", "ankle_temp", "ankle_accel1X", "ankle_accel1Y", "ankle_accel1Z", "ankle_accel2X", "ankle_accel2Y", "ankle_accel2Z", "ankle_gyroX", "ankle_gyroY", "ankle_gyroZ", "ankle_magnetX", "ankle_magnetY", "ankle_magnetZ", "indexedLabel", "predictedLabel")
      .show(20)
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val predictionAccuracy = evaluator.evaluate(predictionResultDF)
    // 模型性能
    println("Testing Error = " + (1.0 - predictionAccuracy))
    // TODO: 执行会有些问题
    val randomForestModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    println("Trained Random Forest Model is:\n" + randomForestModel.toDebugString)
  }
}

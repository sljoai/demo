package com.song.cn.spark.scala.project.badou.model

import com.song.cn.spark.scala.project.badou.feature.{FeatConfig, SimpleFeature}
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.SparkSession

object LRTrain {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Product LR")
      .enableHiveSupport()
      .getOrCreate()

    println("load data from hive...")
    val priors = spark.sql("select * from badou.priors")
    val orders = spark.sql("select * from badou.orders")
    val products = spark.sql("select * from badou.products")
    val trainLabel = spark.sql("select * from badou.trains ")
      .join(orders.filter("eval_set='train'"), "order_id")
      .selectExpr("user_id", "product_id", "1 as label")

    println("feature generating/engineering ...")
    val dfFeat = SimpleFeature.Feat(priors, orders)

    //    加上label（列）数据进行
    val df = dfFeat.join(trainLabel, Seq("user_id", "product_id"), "left_outer")
      .na.fill(0)
      .cache()

    //   只使用user和product特征
    val useUserProdFeat = FeatConfig.userFeatString + "+" + FeatConfig.prodFeatString
    //    加上交叉特征
    val useAddCrossFeat = useUserProdFeat + "+" + FeatConfig.UPFeatString

    //  RFormula实例化并传入参数
    val rformula = new RFormula()
      .setFormula(s"label ~ $useUserProdFeat")
      .setFeaturesCol("features").setLabelCol("label")

    //  RFormula进行数据处理
    val df_ohDone = rformula.fit(df).transform(df).select("features", "label").cache()
    df.unpersist()

    //    实例化lr模型，并设置参数，在这里进行模型的参数调整

    //  注意:在这之后可以尝试使用别的算法测试效果
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0).setStandardization(true).setTol(1e-6)
    //      .setElasticNetParam(0)

    //  将训练数据进行本地训练和模型测试
    val Array(trainingData, testData) = df_ohDone.randomSplit(Array(0.7, 0.3))
    //    lr模型的训练
    val lrModel = lr.fit(trainingData)

    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    //  获得训练总结
    val trainingSummary = lrModel.summary

    // 打印出来每次迭代的Loss
    val objectiveHistory = trainingSummary.objectiveHistory
    objectiveHistory.foreach(loss => println(loss))

    //    获取二分类的评价
    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

    // 以dataframe获取auc areaUnderROC.
    val roc = binarySummary.roc
    //    答应roc表
    roc.show()
    //    答应auc的值,这个是最终利用roc的评价值
    println(binarySummary.areaUnderROC)
  }

}

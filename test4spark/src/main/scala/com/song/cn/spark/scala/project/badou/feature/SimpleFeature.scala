package com.song.cn.spark.scala.project.badou.feature

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object SimpleFeature {
  def Feat(priors: DataFrame, orders: DataFrame): DataFrame = {
    /**
      * product feature:
      * 1. 销售量 prod_cnt
      * 2. 商品被再次购买（reordered）量 prod_sum_rod
      * 3. 统计reordered比率 prod_rod_rate
      */
    //    统计销售量
    val productCnt = priors.groupBy("product_id").count()

    //    统计商品被再次购买量：sum("reordered")，统计reordered比率：avg("reordered")
    val productRodCnt = priors.selectExpr("product_id", "cast(reordered as int)")
      .groupBy("product_id")
      .agg(sum("reordered").as("prod_sum_rod"), avg("reordered").as("prod_rod_rate"))

    val productFeat = productCnt.join(productRodCnt, "product_id")
      .selectExpr("product_id",
        "count as prod_cnt",
        "prod_sum_rod",
        "prod_rod_rate")

    /**
      * user Features：
      * 1.每个用户平均购买订单的间隔周期 user_avg_day_gap
      * 2.每个用户的总订单数量
      * 3.每个用户购买的product商品去重后的集合数据
      * 4.每个用户总商品数量以及去重后的商品数量总商品数量
      * 5.每个用户购买的平均每个订单的商品数量
      * +
      * 6. user
      */
    //     异常值处理：将days_since_prior_order中的空值进行处理
    val ordersNew = orders
      .selectExpr("*", "if(days_since_prior_order='',0,days_since_prior_order) as dspo")
      .drop("days_since_prior_order")
    //    1.每个用户平均购买订单的间隔周期:avg("dspo")
    val userGap = ordersNew.selectExpr("user_id", "cast(dspo as int)")
      .groupBy("user_id")
      .avg("dspo")
      .withColumnRenamed("avg(dspo)", "user_avg_day_gap")
    //    2.每个用户的总订单数量
    val userOrdCnt = orders.groupBy("user_id").count()
    //    3.每个用户购买的product商品去重后的集合数据
    val op = orders.join(priors, "order_id").select("user_id", "product_id")

    //    RDD转DataFrame：需要隐式转换 toDF
    import priors.sparkSession.implicits._

    val userUniOrdRecs = op.rdd.map(x => (x(0).toString, x(1).toString)).groupByKey().mapValues(_.toSet.mkString(",")).toDF("user_id", "product_records")

    //    4. 每个用户总商品数量以及去重后的商品数量
    //    可以将3和4进行合并：同时取到product去重的集合和集合的大小
    val userProRcdSize = op.rdd.map(x => (x(0).toString, x(1).toString)).groupByKey().mapValues { record =>
      val rs = record.toSet
      (rs.size, rs.mkString(","))
    }.toDF("user_id", "tuple").selectExpr("user_id", "tuple._1 as prod_dist_cnt", "tuple._2 as prod_records")
    //    5.每个用户购买的平均每个订单的商品数量
    //    1）先求每个订单的商品数量【对订单做聚合count（）】
    val ordProCnt = priors.groupBy("order_id").count()
    //    2）求每个用户订单中商品个数的平均值【对user做聚合，avg（商品个数）】
    val userPerOrdProdCnt = orders.join(ordProCnt, "order_id").groupBy("user_id").avg("count").withColumnRenamed("avg(count)", "user_avg_ord_prods")

    val userFeat = userGap.join(userOrdCnt, "user_id").join(userProRcdSize, "user_id").join(userPerOrdProdCnt, "user_id").selectExpr("user_id",
      "user_avg_day_gap",
      "count as user_ord_cnt",
      "prod_dist_cnt as user_prod_dist_cnt",
      "prod_records as user_prod_records",
      "user_avg_ord_prods")

    /**
      * user and product Feature: cross feature交叉特征
      * 1. 统计user和对应的product在多少个订单中出现
      * 2. 特定product具体在购物车中出现位置的平均位置
      * 3. 最后一个订单的id
      * 4. 用户对应product在所有用户购买产品量中的占比
      *
      */


    //   把user和product特征（统计量汇总成一个table（DataFrame））
    orders.join(priors, "order_id").select("user_id", "product_id", "eval_set").join(productFeat, "product_id").join(userFeat, "user_id")
  }
}

package com.song.cn.spark.scala.project.badou.feature

object FeatConfig {
  val prodFeatColumn = Array("prod_cnt", "prod_sum_rod", "prod_rod_rate")
  val prodFeatString: String = prodFeatColumn.mkString("+")

  val userFeatColumn = Array("user_avg_day_gap", "user_ord_cnt", "user_prod_dist_cnt", "user_avg_ord_prods")
  val userFeatString: String = userFeatColumn.mkString("+")

  val UPFeatColumn = Array("", "")
  val UPFeatString: String = UPFeatColumn.mkString("+")
}

package com.song.cn.spark.scala.kkb.csvoperator

import java.util.Properties

/**
 * 将CSV数据到处到MySQL中
 */
object CsvOperator {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("sparkCSV")

    val session: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    val frame: DataFrame = session
      .read
      .option("inferSchema", "true")
      .format("csv")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .option("header", "true")
      .option("delimiter","@")
      .option("multiLine","true")
      .option("ignoreLeadingWhiteSpace", true)
      .option("multiLine", true)
      .load("file:////Users/sljoai/Downloads/资料文件夹/爬取数据集/csv文件/zhipin_position_20210205.csv")

    frame.createOrReplaceTempView("job_detail")
    //session.sql("select job_name,job_url,job_location,job_salary,job_company,job_experience,job_class,job_given,job_detail,company_type,company_person,search_key,city from job_detail where job_company = '北京无极慧通科技有限公司'  ").show(80)
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "123456")
    frame.write.mode(SaveMode.Append)
      .jdbc("jdbc:mysql://node03:3306/job_crawel?useUnicode=true&characterEncoding=UTF-8", "job_crawel.jobdetail", prop)
  }

}

package com.song.cn.spark.scala.project.badou

import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

object JiebaKry {
  def main(args: Array[String]): Unit = {
    //    定义结巴分词类的序列化
    val conf = new SparkConf()
      .registerKryoClasses(Array(classOf[JiebaSegmenter]))
      .set("spark.rpc.message.maxSize", "800")
    //    建立sparkSession，并传入定义好的conf
    val spark = SparkSession
      .builder()
      .appName("Jieba Test")
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()


    //    定义结巴分词的方法，传入的是DataFrame，输出的DataFrame会多一列seg（即分好词的一列）
    def jieba_seg(df: DataFrame, colname: String): DataFrame = {
      val segmenter = new JiebaSegmenter()
      val seg = spark.sparkContext.broadcast(segmenter)
      val jieba_udf = udf { (sentence: String) =>
        val segV = seg.value
        segV.process(sentence.toString, SegMode.INDEX)
          .toArray()
          .map(_.asInstanceOf[SegToken].word)
          .filter(_.length > 1)
      }
      //seg列出来的数据都是Array[String]
      df.withColumn("seg", jieba_udf(col(colname)))
    }

    //    从hive中取新闻数据
    val df = spark.sql("select content,label from badou.new_no_seg limit 300")
    val df_seg = jieba_seg(df, "content")
    df_seg.show()
    df_seg.write.mode("overwrite").saveAsTable("badou.news_jieba")

    //    val rdd1 = df.rdd.map(x=>(x(0).toString,x(1).toString))
    //    rdd1.filter(_._1>1)
    //    df.filter(col("content")>1)
  }

}

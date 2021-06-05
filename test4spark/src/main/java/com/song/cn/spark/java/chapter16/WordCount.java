package com.song.cn.spark.java.chapter16;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 单次统计：通过spark-submit提交任务到yarn集群上
 */
public class WordCount {


    public static void main(String[] args) {
        SparkConf sc = new SparkConf().setAppName("WordCount");
        JavaSparkContext javaSc = new JavaSparkContext(sc);
        // 读取输入数据
        JavaRDD<String> inputRdd = javaSc.textFile(args[0]);


        JavaPairRDD<String, Integer> wordContResultRdd = inputRdd
                // 切分单词
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(" ")).iterator();
                    }
                })
                // 将每个单词的次数记录为1
                .mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<>(s, 1);
                    }
                })
                // 聚合操作
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                });
        // 保存统计结果
        wordContResultRdd.saveAsTextFile(args[1]);
    }
}

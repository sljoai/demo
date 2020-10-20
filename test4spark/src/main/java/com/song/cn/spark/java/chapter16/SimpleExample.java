package com.song.cn.spark.java.chapter16;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class SimpleExample {
    

    private static final Logger logger = LoggerFactory.getLogger(SimpleExample.class);

    static {
        PropertyConfigurator.configure("conf/log4j.properties");
    }

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("spark://sparkproject1:7077")
//                .setMaster("local[1]")
                .setAppName("test");
        sparkConf.setJars(new String[]{"D:\\Java\\SRC\\test\\test4spark\\target\\test4spark-1.0-SNAPSHOT-jar-with-dependencies.jar"});

        JavaSparkContext context = new JavaSparkContext(sparkConf);
        //读取输入数据
        JavaRDD inputRDD = context.textFile("hdfs://sparkproject1:9000/user/root/users.txt");
//        JavaRDD inputRDD = context.textFile("file:/D:/Java/SRC/test/README.md");
//        JavaRDD inputRDD = context.textFile("file:/usr/local/spark/README.md");
        System.out.println("-------------------input------------------");

        inputRDD.first();
        //切分单词
        JavaRDD wordCountRDD = inputRDD.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(" ")).iterator();
                    }
                }
        );
        System.out.println("-------------------wordcount------------------");
        wordCountRDD.first();

        //转换为键值对并计数
        JavaPairRDD<String, Integer> pairRDD = wordCountRDD.mapToPair(
                new PairFunction<String, String, Integer>() {

                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<>(s, 1);
                    }
                }
        );
        System.out.println("-------------------pair------------------");
        pairRDD.first();

        //reduce操作
        JavaPairRDD<String, Integer> reduceRDD = pairRDD.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer + integer2;
                    }
                }
        );
        System.out.println("-------------------reduce------------------");
        //reduceRDD.first();
        reduceRDD.saveAsTextFile("hdfs://sparkproject1:9000/user/root/test.txt");
        //TODO: 为啥保存到本地就会找不到输出数据
//        reduceRDD.saveAsTextFile("./data/test.txt");
    }
}

package com.song.cn.flink.nx.window.lesson01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 每隔5秒统计最近10秒的单词出现的次数
 */
public class TimeWindowWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.socketTextStream("hadoop01", 9999);
        dataStream.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String line,
                                Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] fields = line.split(",");
                for(String word:fields){
                    out.collect(Tuple2.of(word,1));
                }

            }
        }).keyBy(0).timeWindow(Time.seconds(10),Time.seconds(5))
                .sum(1)
                .print().setParallelism(1);

        env.execute("word count..");

    }
}

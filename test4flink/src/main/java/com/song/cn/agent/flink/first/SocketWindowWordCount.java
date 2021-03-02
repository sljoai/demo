package com.song.cn.agent.flink.first;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 参考博客：http://wuchong.me/blog/2018/11/07/5-minutes-build-first-flink-application/
 */
public class SocketWindowWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream("localhost",
                9000,
                "\n");

        DataStream<Tuple2<String, Integer>> wordCounts =
                text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (String word : value.split("\\s")) {
                            //第一个字段是单词，第二个字段是次数，次数初始值设置成1
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                });

        DataStream<Tuple2<String, Integer>> windowCounts = wordCounts
                //以单词的Key进行分组
                .keyBy(0)
                //时间窗口，每隔5s聚合一次
                .timeWindow(Time.seconds(10), Time.seconds(2))
                //按照次数字段（即1号索引字段）相加
                .sum(1);
        windowCounts.print().setParallelism(1);
        env.execute("Socket Window WordCount");
    }
}

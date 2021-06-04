package com.song.cn.flink.nx.first;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountWithWebUI {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        // 获取环境变量
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        // 设置并行度
        env.setParallelism(3);

        // 读取数据
        DataStreamSource<String> socketStream = env.socketTextStream("127.0.0.1", 9999);

        // 处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCountSumStream = socketStream.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] lines = line.split(" ");
                        for (String tmp : lines) {
                            collector.collect(new Tuple2<>(tmp, 1));
                        }
                    }
                }).setParallelism(2)
                .keyBy(0)
                .sum(1).setParallelism(1);

        // 输出数据
        wordCountSumStream
                .print()
                // 设置sink operator并行度
                .setParallelism(1);
        env.execute("Word Count with Web UI App.");
    }
}

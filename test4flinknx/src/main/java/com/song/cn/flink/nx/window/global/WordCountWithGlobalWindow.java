package com.song.cn.flink.nx.window.global;

import com.song.cn.flink.nx.window.global.trigger.MyCountTrigger;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 *
 */
public class WordCountWithGlobalWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> socketStream = env.socketTextStream("hadoop01", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> oneWordStream = socketStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] s1 = s.split(" ");
                for (String s2 : s1) {
                    collector.collect(new Tuple2<>(s2, 1));
                }
            }
        });

        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> keyedWindow = oneWordStream.keyBy(0)
                .window(GlobalWindows.create())
                .trigger(CountTrigger.of(3));
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCounts = keyedWindow.sum(1);

        wordCounts.print();

        env.execute("Stream WordCount With Global Window And Trigger.");
    }

}

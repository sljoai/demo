package com.song.cn.flink.nx.window.lesson02;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 每隔5秒统计最近10秒的单词出现的次数
 */
public class TimeWindowWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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
                .process(new MySumProcessWindowFunction()) //foreach key,value -> sum -> key,value
                .print().setParallelism(1);

        env.execute("word count..");


    }

    /**
     * IN, OUT, KEY, W extends Window
     */
    public  static class MySumProcessWindowFunction extends
            ProcessWindowFunction<Tuple2<String,Integer>,Tuple2<String,Integer>
                    ,Tuple,TimeWindow>{
        FastDateFormat dataFormat =FastDateFormat.getInstance("HH:mm:ss");
        @Override
        public void process(Tuple key, Context context,
                            Iterable<Tuple2<String, Integer>> elements,
                            Collector<Tuple2<String, Integer>> out) throws Exception {

            System.out.println("当前系统时间："+ dataFormat.format(System.currentTimeMillis()));
            System.out.println("窗口处理时间："+ dataFormat.format(context.currentProcessingTime()));
            System.out.println("窗口开始时间："+ dataFormat.format(context.window().getStart()));


            int sum=0;
            for (Tuple2<String,Integer> ele:elements){
                sum += 1;
            }
            out.collect(Tuple2.of(key.getField(0),sum));

            System.out.println("窗口结束时间："+ dataFormat.format(context.window().getEnd()));

            System.out.println("=====================================================");
        }
    }





}

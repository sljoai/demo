package com.song.cn.flink.nx.window.lesson06;



import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

/**
 * 需求：每隔5秒计算最近10秒的单词次数
 *
 * 引入watermark解决问题
 */
public class WindowWordCountByWaterMark {
    public static void main(String[] args) throws  Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //步骤一：设置时间类型，默认的是Processtime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> dataStream = env.addSource(new TestSource());
        dataStream.map(new MapFunction<String, Tuple2<String,Long>>() {
            @Override
            public Tuple2<String, Long> map(String line) throws Exception {
                String[] fields = line.split(",");
                return new Tuple2<>(fields[0],Long.valueOf(fields[1]));
            }
        }).assignTimestampsAndWatermarks(new EventTimeExtractor())
                .keyBy(0)
                .timeWindow(Time.seconds(10),Time.seconds(5))
                .process(new SumProcessFunction()).print().setParallelism(1);


        env.execute("WindowWordCountAndTime");

    }

    public static class TestSource implements
            SourceFunction<String>{
        FastDateFormat dateformat =  FastDateFormat.getInstance("HH:mm:ss");
        @Override
        public void run(SourceContext<String> cxt) throws Exception {
            String currTime = String.valueOf(System.currentTimeMillis());
            while(Integer.valueOf(currTime.substring(currTime.length() - 4)) > 100){
                currTime=String.valueOf(System.currentTimeMillis());
                continue;
            }
            System.out.println("开始发送事件的时间："+dateformat.format(System.currentTimeMillis()));
            TimeUnit.SECONDS.sleep(3);
            String event="hadoop,"+System.currentTimeMillis();
            cxt.collect(event);

            TimeUnit.SECONDS.sleep(3);
            cxt.collect("hadoop,"+System.currentTimeMillis());

            TimeUnit.SECONDS.sleep(3);
            cxt.collect(event);
            TimeUnit.SECONDS.sleep(3000);




        }

        @Override
        public void cancel() {

        }
    }


    private static class EventTimeExtractor implements AssignerWithPeriodicWatermarks<Tuple2<String,Long>>{
        //设置 5s的延迟（乱序）
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            //System.out.println("water maker:......"+System.currentTimeMillis());
            return new Watermark(System.currentTimeMillis() - 5000);
        }

        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long l) {
            return element.f1;
        }
    }

    /**
     * IN
     * OUT
     * KEY
     * W extends Window
     *
     */
    public static class  SumProcessFunction
            extends ProcessWindowFunction<Tuple2<String,Long>,Tuple2<String,Integer>,Tuple,TimeWindow>{

        FastDateFormat dataformat=FastDateFormat.getInstance("HH:mm:ss");
        @Override
        public void process(Tuple tuple, Context context,
                            Iterable<Tuple2<String, Long>> allElements,
                            Collector<Tuple2<String, Integer>> out) {
            System.out.println("当前系统时间："+dataformat.format(System.currentTimeMillis()));
            System.out.println("窗口处理时间："+dataformat.format(context.currentProcessingTime()));
            System.out.println("窗口开始时间："+dataformat.format(context.window().getStart()));
            System.out.println("窗口结束时间："+dataformat.format(context.window().getEnd()));

            System.out.println("=====================================================");

            int count=0;
           for (Tuple2<String,Long> e:allElements){
               count++;
           }
            out.collect(Tuple2.of(tuple.getField(0),count));

        }
    }
}

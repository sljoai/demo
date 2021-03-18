package com.song.cn.flink.nx.window.lesson10;



import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * 需求：3秒一个窗口，进行单词计数
 *
 * 多并行度下多watermark
 *
 * 000001,1461756870000
 * 000001,1461756883000
 * 000001,1461756888000
 *
 */
public class WindowWordCountByWaterMark5 {
    public static void main(String[] args) throws  Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //步骤一：设置时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(2);

        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 8888);


        OutputTag<Tuple2<String, Long>> outputTag = new OutputTag<Tuple2<String, Long>>("late-date"){};

        SingleOutputStreamOperator<String> result = dataStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String line) throws Exception {
                String[] fields = line.split(",");
                return new Tuple2<>(fields[0], Long.valueOf(fields[1]));
            }
            //步骤二：获取数据里面的event Time
        }).assignTimestampsAndWatermarks(new EventTimeExtractor())
                .keyBy(0)
                .timeWindow(Time.seconds(3))
//                .allowedLateness(Time.seconds(2)) // 允许事件迟到 2 秒
                .sideOutputLateData(outputTag) //保留迟到太多的数据
                .process(new SumProcessWindowFunction());
        //打印结果数据
        result.print();



        result.getSideOutput(outputTag).map(new MapFunction<Tuple2<String,Long>, String>() {
            @Override
            public String map(Tuple2<String, Long> stringLongTuple2) throws Exception {
                return "迟到数据："+stringLongTuple2.toString();
            }
        }).print();

        env.execute("WindowWordCountByWaterMark3");

    }

    /**
     * IN, OUT, KEY, W
     * IN：输入的数据类型
     * OUT：输出的数据类型
     * Key：key的数据类型（在Flink里面，String用Tuple表示）
     * W：Window的数据类型
     */
    public static class SumProcessWindowFunction extends
            ProcessWindowFunction<Tuple2<String,Long>,String,Tuple,TimeWindow> {
        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");
        /**
         * 当一个window触发计算的时候会调用这个方法
         * @param tuple key
         * @param context operator的上下文
         * @param elements 指定window的所有元素
         * @param out 用户输出
         */
        @Override
        public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements,
                            Collector<String> out) {
            System.out.println("处理时间：" + dateFormat.format(context.currentProcessingTime()));
            System.out.println("window start time : " + dateFormat.format(context.window().getStart()));

            List<String> list = new ArrayList<>();
            for (Tuple2<String, Long> ele : elements) {
                list.add(ele.toString() + "|" + dateFormat.format(ele.f1));
            }
            out.collect(list.toString());
            System.out.println("window end time  : " + dateFormat.format(context.window().getEnd()));

        }
    }

    private static class EventTimeExtractor
            implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {
        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");
        private long currentMaxEventTime=0L;
        private long maxOufOfOrderness=10000;//最大乱序时间
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxEventTime - maxOufOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long timeStamp) {
            Long currentElementTime = element.f1;
            currentMaxEventTime = Math.max(currentMaxEventTime,currentElementTime);

            long id = Thread.currentThread().getId();

            System.out.println("当前线程id="+id+" event = " + element
                    + "|" + dateFormat.format(element.f1) // Event Time
                    + "|" + dateFormat.format(currentMaxEventTime)  // Max Event Time
                    + "|" + dateFormat.format(getCurrentWatermark().getTimestamp())); // Current Watermark

            return currentElementTime;
        }
    }
}

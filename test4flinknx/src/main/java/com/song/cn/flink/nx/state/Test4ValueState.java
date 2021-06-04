package com.song.cn.flink.nx.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 需求: 将两条数据合并成一条数据输出
 * 1.自定义 Sink -> 存储上一次的数据到State中
 * 2.需要一个Operator State -> ListState
 *
 */
public class Test4ValueState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Long>> dataStream = env.fromElements(
                Tuple2.of("spark", 3L),
                Tuple2.of("hadoop", 4L),
                Tuple2.of("flink", 5L),
                Tuple2.of("hadoop", 2L),
                Tuple2.of("spark", 7L),
                Tuple2.of("flink", 5L));

        dataStream.addSink(new CustomSink(2))
                // 这个是必须的，否则无法输出结果
                .setParallelism(1);

        env.execute("Test4OperatorState");
    }
}
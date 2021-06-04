package com.song.cn.flink.nx.state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class CountWindowSumWithReducingState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    ReducingState<Long> sumState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 注册State状态
        ReducingStateDescriptor<Long> reducingStateDescriptor = new ReducingStateDescriptor<>(
                "sum",
                new ReduceFunction<Long>() { // 聚合函数
                    @Override
                    public Long reduce(Long value1, Long value2) throws Exception {
                        return value1 + value2;
                    }
                }
                , Long.class);
        sumState = getRuntimeContext().getReducingState(reducingStateDescriptor);
    }


    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
        // 将数据放到状态中
        sumState.add(value.f1);
        out.collect(Tuple2.of(value.f0, sumState.get()));
    }
}

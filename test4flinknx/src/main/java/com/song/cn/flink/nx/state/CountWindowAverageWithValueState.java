package com.song.cn.flink.nx.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * 自定义 WordCount Value State
 */
public class CountWindowAverageWithValueState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {
    /**
     * 用以保存每个key出现的次数，以及这个key对应的value的总值
     * f0: 用于存储key出现的次数
     * f1: 用于存储key的value的总值
     */
    private ValueState<Tuple2<Long, Long>> countAndSum;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 注册状态
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>(
                "average", // 状态的名称
                Types.TUPLE(Types.LONG, Types.LONG)); //状态存储的数据类型
        countAndSum = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> longLongTuple2, Collector<Tuple2<Long, Double>> collector) throws Exception {
        // 拿到当前key的状态值
        Tuple2<Long, Long> currentValue = countAndSum.value();

        // 如果状态值还没有初始化，则初始化
        if (null == currentValue) {
            currentValue = Tuple2.of(0L, 0L);
        }

        // 更新状态中的元素个数
        currentValue.f0 += 1;

        // 更新状态中的元素总值
        currentValue.f1 += longLongTuple2.f1;

        // 更新元素的状态
        countAndSum.update(currentValue);

        // 判断：如果key的次数超过3次，则输出其平均值
        // TODO: 待确认，是只需要输出一次嘛？如果需要持续输出呢？
        if (currentValue.f0 >= 3) {
            double average = (double) currentValue.f1 / currentValue.f0;
            // 输出key及其平均值
            collector.collect(Tuple2.of(longLongTuple2.f0, average));
            countAndSum.clear();
        }

    }
}


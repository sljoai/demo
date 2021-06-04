package com.song.cn.flink.nx.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.List;

public class CountWindowAverageWithListState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

    private ListState<Tuple2<Long, Long>> elementByKey;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 注册状态
        ListStateDescriptor<Tuple2<Long, Long>> average = new ListStateDescriptor<>(
                "average",
                Types.TUPLE(Types.LONG, Types.LONG));
        elementByKey = getRuntimeContext().getListState(average);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Double>> out) throws Exception {
        // 拿到当前key的状态值
        // TODO: 是怎么知道获取就是当前key的状态值的
        Iterable<Tuple2<Long, Long>> currentKeyElement = elementByKey.get();
        if (currentKeyElement == null) {
            elementByKey.addAll(Collections.emptyList());
        }

        // 更新状态
        elementByKey.add(value);

        // 判断，如果当前的 key 出现了 3 次，则需要计算平均值，并且输出
        List<Tuple2<Long, Long>> allElements = Lists.newArrayList(elementByKey.get());
        if (allElements.size() >= 3) {
            long count = 0;
            long sum = 0;
            for (Tuple2<Long, Long> element : allElements) {
                count++;
                sum += element.f1;
            }
            double average = (double) sum/count;
            out.collect(Tuple2.of(value.f0,average));
            elementByKey.clear();
        }

    }
}

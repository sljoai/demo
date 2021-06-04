package com.song.cn.flink.nx.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.UUID;

public class CountWindowAverageWithMapState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

    MapState<String, Long> elementByKey;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Long> mapStateDescriptor = new MapStateDescriptor<>(
                "average",
                String.class,
                Long.class
        );
        elementByKey = getRuntimeContext().getMapState(mapStateDescriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Double>> out) throws Exception {
        elementByKey.put(UUID.randomUUID().toString(), value.f1);
        // 判断，如果当前的 key 出现了 3 次，则需要计算平均值，并且输出
        List<Long> allElements = Lists.newArrayList(elementByKey.values());
        if (allElements.size() >= 3) {
            long count = 0;
            long sum = 0;
            for (Long element : allElements) {
                count++;
                sum += element;
            }
            double avg = (double) sum / count;
            out.collect(Tuple2.of(value.f0, avg));
            // 清除状态
            elementByKey.clear();
        }
    }
}

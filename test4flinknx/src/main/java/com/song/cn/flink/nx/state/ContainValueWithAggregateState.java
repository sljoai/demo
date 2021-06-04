package com.song.cn.flink.nx.state;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class ContainValueWithAggregateState extends RichFlatMapFunction<Tuple2<Long,Long>,Tuple2<Long,String>> {
    /**
     * key
     * value: and 连接字符串
     */
    private AggregatingState<Long,String> totalStr;
    @Override
    public void open(Configuration parameters) throws Exception {
        AggregatingStateDescriptor<Long, String, String> totalStrAggregate = new AggregatingStateDescriptor<>(
                "totalStr",
                new AggregateFunction<Long, String, String>() {
                    @Override
                    public String createAccumulator() {
                        return "Contains: ";
                    }

                    @Override
                    public String add(Long value, String accumulator) {
                        if ("Contains: ".equals(accumulator)) {
                            return accumulator + value;
                        }
                        return accumulator + " and " + value;
                    }

                    @Override
                    public String getResult(String accumulator) {
                        return accumulator;
                    }

                    @Override
                    public String merge(String a, String b) {
                        return a + " and " + b;
                    }
                }, String.class);
       totalStr  = getRuntimeContext().getAggregatingState(totalStrAggregate);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, String>> out) throws Exception {
        totalStr.add(value.f1);
        out.collect(Tuple2.of(value.f0,totalStr.get()));
    }
}

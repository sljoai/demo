package com.song.cn.flink.nx.state.order;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class EnrichmentFunction extends RichCoFlatMapFunction<OrderInfo1, OrderInfo2, Tuple2<OrderInfo1,OrderInfo2>> {

    private ValueState<OrderInfo1> orderInfo1ValueState;

    private ValueState<OrderInfo2> orderInfo2ValueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<OrderInfo1> orderInfo1ValueStateDescriptor = new ValueStateDescriptor<>(
                "order-info-1",
                OrderInfo1.class
        );
        orderInfo1ValueState = getRuntimeContext().getState(orderInfo1ValueStateDescriptor);

        ValueStateDescriptor<OrderInfo2> orderInfo2ValueStateDescriptor = new ValueStateDescriptor<>(
                "order-info-2",
                OrderInfo2.class
        );
        orderInfo2ValueState = getRuntimeContext().getState(orderInfo2ValueStateDescriptor);
    }

    @Override
    public void flatMap1(OrderInfo1 value, Collector<Tuple2<OrderInfo1,OrderInfo2>> out) throws Exception {
        OrderInfo2 orderInfo2 = orderInfo2ValueState.value();
        if(orderInfo2 !=null){
            orderInfo2ValueState.clear();
            out.collect(Tuple2.of(value,orderInfo2));
        }else{
            orderInfo1ValueState.update(value);
        }
    }

    @Override
    public void flatMap2(OrderInfo2 value, Collector<Tuple2<OrderInfo1,OrderInfo2>> out) throws Exception {
        OrderInfo1 orderInfo1 = orderInfo1ValueState.value();
        if(orderInfo1 != null){
            orderInfo1ValueState.clear();
            out.collect(Tuple2.of(orderInfo1,value));
        }else {
            orderInfo2ValueState.update(value);
        }
    }
}

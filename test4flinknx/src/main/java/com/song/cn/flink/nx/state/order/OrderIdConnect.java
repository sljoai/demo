package com.song.cn.flink.nx.state.order;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OrderIdConnect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> orderInfoStream1 = env.addSource(new FileSource(Constants.ORDER_INFO_FILE_PATH1));

        DataStreamSource<String> orderInfoStream2 = env.addSource(new FileSource(Constants.ORDER_INFO_FILE_PATH2));

        KeyedStream<OrderInfo1, String> orderInfoKeyStream1 = orderInfoStream1
                .map(line -> OrderInfo1.str2OrderInfo1(line))
                .keyBy(orderInfo1 -> orderInfo1.getOrderId());

        KeyedStream<OrderInfo2, String> orderInfoKeyedStream2 = orderInfoStream2
                .map(line -> OrderInfo2.str2OrderInfo2(line))
                .keyBy(orderInfo2 -> orderInfo2.getOrderId());
        orderInfoKeyStream1
                .connect(orderInfoKeyedStream2)
                .flatMap(new EnrichmentFunction())
                .print();

        env.execute("Order Connect by OrderId.");

    }
}

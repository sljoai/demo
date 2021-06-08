package com.song.cn.agent.flink.state.twopc.kafka;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaE2EExactlyOnceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /** 初始化 Consumer 配置 */
        Properties consumerConfig = new Properties();
        consumerConfig.setProperty("bootstrap.servers", "127.0.0.1:9091");
        consumerConfig.setProperty("group.id", "flink_poc_k110_consumer");
        /** 初始化 Kafka Consumer */
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<String>(
                "flink_kafka_poc_input",
                new SimpleStringSchema(), consumerConfig);
        /** 将 Kafka Consumer 加入到流处理 */
        DataStream<String> stream = env.addSource(flinkKafkaConsumer);
        /** 初始化 Producer 配置 */Properties producerConfig = new Properties();
        producerConfig.setProperty("bootstrap.servers", "127.0.0.1:9091");
        /** 初始化 Kafka Producer */
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>("flink_kafka_poc_output",
                new MapSerialization(), producerConfig);/** 将 Kafka Producer 加入到流处理 */
        stream.addSink(myProducer);
        /** 执行 */
        env.execute();
    }
}

class MapSerialization implements SerializationSchema<String> {
    public byte[] serialize(String element) {
        return element.getBytes();
    }
}

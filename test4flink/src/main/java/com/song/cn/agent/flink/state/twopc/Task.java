package com.song.cn.agent.flink.state.twopc;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * 消费kafka消息，sink(自定义)到mysql中，保证kafka to mysql的Exactly-Once
 */
@SuppressWarnings("all")
public class Task {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        //checkpoint的周期
        env.enableCheckpointing(10000);
        //设置模式为：exactly_one，仅一次语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //checkpoint最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        //检查点必须在1分钟之内完成，或者被丢弃【checkpoint超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        //同一时间只允许进行一次检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //表示一旦Flink程序被cancel后，会保留checkpoint数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setStateBackend(new FsStateBackend("file:///opt/data/"));
        //设置kafka消费参数
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.80.235:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-k2m-g1");

        FlinkKafkaConsumer010<ObjectNode> kafkaConsumer011 = new FlinkKafkaConsumer010<>("student", new
                JSONKeyValueDeserializationSchema(true), props);

        //!!!!如果防止 写入mysql数据丢失，则不能使用此代码
//        kafkaConsumer011.setStartFromLatest();

        //加入kafka数据源
        DataStreamSource<ObjectNode> streamSource = env.addSource(kafkaConsumer011).setParallelism(1);

        //数据传输到下游
        streamSource.addSink(new Sink()).name("MST2").setParallelism(1);

        env.execute(Task.class.getName());

    }
}

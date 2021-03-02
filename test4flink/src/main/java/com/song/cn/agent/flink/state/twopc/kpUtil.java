package com.song.cn.agent.flink.state.twopc;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class kpUtil {

    public static final String brokerServers = "192.168.80.235:9092";

    public static final String topic = "student";

    public static void main(String[] args) throws Exception {
        sendMessage();
    }

    private static void sendMessage() throws Exception {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerServers);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.ACKS_CONFIG, "1");
        prop.put(ProducerConfig.BATCH_SIZE_CONFIG, "10");
        prop.put(ProducerConfig.RETRIES_CONFIG, 2);

        KafkaProducer producer = new KafkaProducer(prop);

        //循环发送
        for (int i = 100; i <= 2000; i++) {
            Student student = new Student(i, "name_" + i, "pwd_" + i, 10 + i);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, JSON.toJSONString(student));
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata md, Exception ex) {
                    if (ex != null) {
                        ex.printStackTrace();
                        if (md != null) {
                            System.err.println("offset:" + md.offset());
                        } else {
                            System.err.println("metadata is null");
                        }
                    } else {
                        String data = "sending msg => topic: " + md.topic() + ",partition: " + md.partition() +
                                ",offset: " + md.offset() + ",value: " + record.value();
                        System.out.println(data);
                    }

                }
            });
            Thread.sleep(1000);
        }
        producer.flush();
        producer.close();
    }
}

package com.toonew.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 自动提交偏移方式，还有其他几种方式
 */
public class KafkaCon2 {

    public static void main(String[] args) {

//        String topicName = args.length == 0 ? "toonew-topic" : args[0];
        String topicName = args.length == 0 ? "test15" : args[0];

        Properties props = new Properties();

//        props.put("bootstrap.servers", "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094");
        props.put("bootstrap.servers", "192.168.71.25:9092,192.168.71.26:9092,192.168.71.27:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");            //自动提交
        props.put("auto.commit.interval.ms", "1000");       //自动提交频率
        props.put("session.timeout.ms", "30000");           //
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList("foo", "bar"));

        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {
                System.out.println(buffer.toString());
                consumer.commitSync();
                buffer.clear();
            }
        }

    }

}

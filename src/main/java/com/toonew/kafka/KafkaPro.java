package com.toonew.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * 生产者包括一个缓冲区池，它保存尚未发送到服务器的记录，以及一个后台I/O线程，负责将这些记录转换为请求并将其传输到集群。
 * 使用后未能关闭生产者将泄漏这些资源。
 * kafka 客户端版本1.0.0
 * http://blog.csdn.net/isea533/article/details/73822881  （有一定的参考意义)
 */
public class KafkaPro {
    //    private static String[] RandomString = new String[]{"hello", "kafka", "storm", "spark", "test message", "another test message"};
    private static String[] RandomString = new String[]{
            "the hello test test test2 test3 test4",
            "kafka te",
            "storm test",
            "spark the test45",
            "test message tets",
            "another the test message"};

    public static void main(String[] args) {
        String topicName = args.length == 0 ? "toonew-test" : args[0];

        Properties props = new Properties();
//        props.put("bootstrap.servers", "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094");
        props.put("bootstrap.servers", "192.168.71.25:9092,192.168.71.26:9092,192.168.71.27:9092");
        props.put("ack", "all");              //acks配置其请求被视为完整性的标准,"all"意味着领导者将等待完整的同步副本来确认记录
        props.put("retries", 0);              //请求失败情况下 重试次数
        props.put("batch.size", 16384);       //维护每个分区的未发送出去的缓冲区
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432); //生产者可用于缓冲的总内存量
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10; i++)
            producer.send(new ProducerRecord<>(topicName,
                    Integer.toString(i), RandomString[new Random().nextInt(RandomString.length - 1)]));
        System.out.println("Message sent successfully");
        producer.close();
    }

}

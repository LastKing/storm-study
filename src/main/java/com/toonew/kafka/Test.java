package com.toonew.kafka;

import com.toonew.test1.blot.WordCounter;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.UUID;

/**
 * 自官方文档：
 * http://storm.apachecn.org/releases/cn/1.1.0/storm-kafka.html
 * 整合的例子
 * https://www.w3cschool.cn/apache_kafka/apache_kafka_integration_storm.html
 */
public class Test {
    private static String zkUrl = "192.168.71.25:2181,192.168.71.26:2181,192.168.71.27:2181";        // the defaults.
    private static String brokerUrl = "192.168.71.25:9092,192.168.71.26:9092,192.168.71.27:9092";


    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafka-spout", kaSpout(), 5);
        builder.setBolt("word-split", new SplitBolt())
                .shuffleGrouping("kafka-spout");
        builder.setBolt("word-count", new WordCounter())
                .shuffleGrouping("word-split");

        Config config = new Config();
        config.setDebug(true);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("kafka-storm-top", config, builder.createTopology());

        Thread.sleep(1000);
        cluster.shutdown();

    }

    /**
     * 如何生成一个kafka
     */
    private static KafkaSpout kaSpout() {
        //1.通过zookeeper 动态获取 kafka的信息
        String zkUrl = "localhost:2181";//本地测试
//        String zkUrl = "192.168.71.25:2181,192.168.71.26:2181,192.168.71.27:2181";
        BrokerHosts hosts = new ZkHosts(zkUrl, "/brokers");

        String topicName = "toonew-topic";

        //2.spoutConfig 继承自 kafkaConfig   设置kafka的相关信息
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
        //3.设置将kafka中的byteBuffer  转成  string（StringScheme)
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.startOffsetTime = -1;// -2 从kafka头开始 -1 是从最新的开始 0 =无 从ZK开始 正式环境的
//        config.startOffsetTime = -1;// -2 从kafka头开始 -1 是从最新的开始 0 =无 从ZK开始 本地环境的
        spoutConfig.ignoreZkOffsets = false;
        return new KafkaSpout(spoutConfig);
    }

}

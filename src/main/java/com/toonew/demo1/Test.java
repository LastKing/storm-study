package com.toonew.demo1;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * 最基本的 storm 流程
 * inputStream -->  spout -->  blot
 * 这里只有 输入 和 计算 的操作，怎么获取运算的结果嘞？
 * LocalCluster  本地模式（在jvm 中模拟storm 集群运行）
 */
public class Test {

    public static void main(String[] args) {
        try {
            //1.创建一个  topology 生成者
            TopologyBuilder builder = new TopologyBuilder();

            //2.设置spout 和 bolt
            builder.setSpout("1", new TestWord1Spout(true), 5);
            builder.setSpout("2", new TestWord1Spout(true), 3);
            builder.setBolt("3", new TestWordCounter1(), 3)
                    .fieldsGrouping("1", new Fields("word"))
                    .fieldsGrouping("2", new Fields("word"));
            builder.setBolt("4", new TestGlobalCount1())
                    .globalGrouping("3");

            //3.设置Config
            Config conf = new Config();
            conf.setNumWorkers(4);
            conf.setDebug(true);

            //远程模式（将topology 提交到storm集群中）
//            StormSubmitter.submitTopology("toonewtopology", conf, builder.createTopology());

            //本地模式
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("mytopology", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
package com.toonew.trident;

import com.toonew.trident.aggregate.Count;
import com.toonew.trident.aggregate.CountAsAggregator;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * TridentTopology
 * 最基本的例子
 * 只包含了 spout 、 blot  和 聚合aggregate 部分
 * 这个模式还没特别搞懂 它的api
 * 貌似是对普通 storm  api的一个高层次封装
 * http://storm.apachecn.org/releases/cn/1.1.0/Trident-tutorial.html
 */
public class Standard1 {

    public static void main(String[] args) throws Exception {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"));

        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology.newStream("spout1", spout)
                .each(new Fields("sentence"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
//                .partitionAggregate(new Fields("word"), new CountAsAggregator(), new Fields("count2"))
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("word"), new Count(), new Fields("count"))
                .parallelismHint(6);

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("trident-study", config, topology.build());

        Thread.sleep(3000);
        cluster.killTopology("trident-study");
        cluster.shutdown();
        cluster.shutdown();
    }

}

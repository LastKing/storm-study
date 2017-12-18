package com.toonew.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class TestDrpc {

    public static void main(String[] args) throws Exception {
//        TridentTopology topology = new TridentTopology();
//        LocalDRPC drpc = new LocalDRPC();
//
//        topology.newDRPCStream("words")
//                .each(new Fields("args"), new Split(), new Fields("word"))
//                .groupBy(new Fields("word"))
//                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
//                .each(new Fields("count"), new FilterNull())
//                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));
//
//        Config config = new Config();
//        config.setDebug(true);
//
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("trident-study", config, topology.build());
//
//        Thread.sleep(5000);
//        cluster.killTopology("trident-study");
//        cluster.shutdown();
    }

}
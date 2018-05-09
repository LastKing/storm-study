package com.toonew.ssp.project;

import com.toonew.trident.state_mongo.MongoMapState;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * 根据 key 计算总数
 */
public class Demo {

    public static void main(String[] args) throws Exception {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("1_1_3.0"),
                new Values("1_1_4.0"),
                new Values("1_1_5.0"),
                new Values("1_1_6.0"));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        TridentState wordCounts =
                topology.newStream("spout1", spout)
                        .each(new Fields("sentence"), new Split1(), new Fields("word", "price"))
                        .groupBy(new Fields("word"))
                        .persistentAggregate(new MongoMapState.Factory(), new Fields("word", "price"), new Count(), new Fields("count"))
                        .parallelismHint(6);

        LocalDRPC drpc = new LocalDRPC();

        topology.newDRPCStream("words", drpc)                                       //drpc function名，指定查询名
                .each(new Fields("args"), new Split(), new Fields("word"))                  //处理上传过来的数据args
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .aggregate(new Fields("count"), new ResultAggregator(), new Fields("sum"));

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("trident-study", config, topology.build());

        Thread.sleep(15000);
        System.out.println("query key word by drpc:" + drpc.execute("words", "1_1"));//(function name,args)
        Thread.sleep(3000);
        cluster.killTopology("trident-study");
        drpc.shutdown();
        cluster.shutdown();
    }

}
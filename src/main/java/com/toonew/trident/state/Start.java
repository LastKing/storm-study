package com.toonew.trident.state;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * http://storm.apachecn.org/releases/cn/1.1.0/Trident-state.html
 * 这并不是完整可以演示的例子
 */
public class Start {

    public static void main(String[] args) throws Exception {
        FixedBatchSpout locationsSpout = new FixedBatchSpout(new Fields("userid", "location"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"));

        TridentTopology topology = new TridentTopology();

        TridentState locations =
                topology.newStream("spouts", locationsSpout)
                        .partitionPersist(new LocationDBFactory(), new Fields("userid", "location"), new LocationUpdater());

        LocalDRPC drpc = new LocalDRPC();
        topology.newDRPCStream("myspout", drpc)   //这里原生的例子是一个普通的spout懒得麻烦在实现一个 就改用drpc的api
                .each(new Fields("args"), new Split(), new Fields("userid"))
                .stateQuery(locations, new Fields("userid"), new QueryLocation(), new Fields("location"));

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("trident-study", config, topology.build());

        Thread.sleep(8000);
        System.out.println("query key word by drpc:" + drpc.execute("userid", "the cow"));//(function name,args)
        Thread.sleep(3000);
        cluster.killTopology("trident-study");
        drpc.shutdown();
        cluster.shutdown();
    }

}

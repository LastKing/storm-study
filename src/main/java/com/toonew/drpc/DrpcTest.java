package com.toonew.drpc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.DRPCSpout;
import org.apache.storm.drpc.ReturnResults;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * 一个storm 标准模式下的 drpc 新版的 实现例子
 * 这个貌似挂钩与spout上实现，但是怎么查询别的state 怎么查询。。还待理解
 */
public class DrpcTest {
//    private static Log log = LogFactory.getLog(DrpcTest.class);

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        LocalDRPC drpc = new LocalDRPC();//本地RPC

        //构建topology
        DRPCSpout drpcSpout = new DRPCSpout("exclation", drpc);
        builder.setSpout("drpc", drpcSpout, 2);
        builder.setBolt("exam", new ExclamationBolt(), 4).shuffleGrouping("drpc");
        builder.setBolt("return", new ReturnResults(), 4).shuffleGrouping("exam");

        //配置topology
        Config config = new Config();
        config.setDebug(true);
        config.setMaxSpoutPending(1000);
        config.setNumWorkers(2);

        //本地集群
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("test", config, builder.createTopology());

        //本地和storm交互
        System.out.println("-------------" + drpc.execute("exclation", "luo") + "-------------");
        System.out.println("-------------" + drpc.execute("exclation", "lky") + "-------------");


        Utils.sleep(1000 * 10);
        drpc.shutdown();
        localCluster.killTopology("test");
        localCluster.shutdown();
    }

    public static class ExclamationBolt extends BaseBasicBolt {
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("result", "return-info"));
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String arg = tuple.getString(0);
            Object retInfo = tuple.getValue(1);
//            log.info("execute----------->"+arg+"--------"+retInfo);
            collector.emit(new Values(arg + "!!!", retInfo));
        }
    }

}

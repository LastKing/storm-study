package com.toonew.drpc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.security.InvalidParameterException;
import java.util.Map;

/**
 * 当前storm 版本1.1.0 （这使用的api应该是更加古老的）
 * 例子出处：http://wiki.jikexueyuan.com/project/storm/topology.html
 * 详解 参考官方文档：
 * http://storm.apachecn.org/releases/cn/1.1.0/Distributed-RPC.html
 * 此例子为 本地模式，非remote 模式，前提远程需要开启服务器
 */
public class OldDrpcTest {

    public static void main(String[] args) {
        //创建一个本地drpc
        LocalDRPC drpc = new LocalDRPC();

        //创建以topology   ( LinearDRPCTopologyBuilder  帮助我们自动集成了DRPCSpout和returnResults(bolt) )
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("add");
        builder.addBolt(new AdderBolt(), 2);

        //设置config
        Config conf = new Config();
        conf.setDebug(false);

        //本地运行  通过drpc 创建
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("drpcder-topology", conf,
                builder.createLocalTopology(drpc));//提交时，追加绑定的drpc
//                builder.createRemoteTopology();//提交时，追加绑定的drpc

        //通过drpc 发送需要计算的数据，并获得结果
        String result = drpc.execute("add", "1+-1");
        System.out.println("add 1+-1:" + result);

        result = drpc.execute("add", "1+1+5+10");
        System.out.println("add 1+1+5+10:" + result);

        cluster.shutdown();
        drpc.shutdown();
    }


    static class AdderBolt implements IRichBolt {
        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            String[] numbers = input.getString(1).split("\\+");
            Integer added = 0;
            if (numbers.length < 2) {
                throw new InvalidParameterException("Should be at least 2 numbers");
            }
            for (String num : numbers) {
                added += Integer.parseInt(num);
            }
            collector.emit(new Values(input.getValue(0), added));
        }

        @Override
        public void cleanup() {

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "result"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }
}


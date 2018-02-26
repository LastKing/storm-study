package com.toonew.trident;

import com.toonew.trident.aggregate.Count;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * trident + drpc 操作
 * 该例子相关比较完整
 * 包含 spout 、blot 、aggregate 、state 和 drpc 基本都用上了
 * 理解 storm下的  state 与 值的传递，以及drpc 查询任务的创建以及结果的返回
 * 这个为本地模式demo
 */
public class Standard2 {

    public static void main(String[] args) throws Exception {
        //1.生成输入spout
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"));
        spout.setCycle(true);

        //2.创建计划
        TridentTopology topology = new TridentTopology();
        //3.将spout1 计算完毕，并生成 state  用于以后查询  （http://www.cnblogs.com/hseagle/p/3516458.html 按照该文章中的图片理解）
        TridentState wordCounts =
                topology.newStream("spout1", spout)
                        .each(new Fields("sentence"), new Split(), new Fields("word"))
                        .groupBy(new Fields("word"))
                        .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                        .parallelismHint(6);

        //3.创建本地测试 需要使用的drpc连接
        LocalDRPC drpc = new LocalDRPC();

        //4.创建一个drpc数据流（本地测试需要绑定LocalDrpc，remote storm 不需要传入drpc 会自动处理这个drpc）
        topology.newDRPCStream("words", drpc)                                        //drpc function名，指定查询名
                .each(new Fields("args"), new Split(), new Fields("word"))                   //处理上传过来的数据args
                .groupBy(new Fields("word"))                                                 //
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));

        Config config = new Config();
        config.setDebug(true);

        //5.创建本地运行 storm 单例jvm 环境，并提交 topology
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("trident-study", config, topology.build());

        Thread.sleep(8000);
        //6.通过drpc execute 执行查询
        System.out.println("query key word by drpc:" + drpc.execute("words", "the cow"));//(function name,args)
        Thread.sleep(3000);
        cluster.killTopology("trident-study");
        drpc.shutdown();
        cluster.shutdown();
    }

}
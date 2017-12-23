package com.toonew.trident;

import com.hazelcast.core.Hazelcast;
import com.toonew.trident.blot.WordSplit;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.shade.org.apache.commons.collections.MapUtils;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.TransactionalValue;
import org.apache.storm.trident.state.map.IBackingMap;
import org.apache.storm.trident.state.map.TransactionalMap;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * trident 集成 kafka drpc 实现
 * 如果完全没入门请先参考standard1 （理解trident 和 drpc）
 * http://blog.csdn.net/jinhong_lu/article/details/46766195
 */
public class KafkaDrpcTrident {

    public static void main(String[] args) throws Exception {
        //1.storm 配置文件
        Config config = new Config();
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

        //2.根据 args 判断 remote model and local model  （drpc方法解释请看drpc package）
        if (args.length > 1) {
            String name = args[1];
            String dockerIp = args[2];
            config.setNumWorkers(2);
            config.setMaxTaskParallelism(5);

            config.put(Config.NIMBUS_SEEDS, dockerIp);
            config.put(Config.NIMBUS_THRIFT_PORT, 6627);
            config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
            config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(dockerIp));

            StormSubmitter.submitTopology(name, config, KafkaDrpcTrident.buildTopology());
        } else {
            LocalDRPC drpc = new LocalDRPC();
            config.setNumWorkers(2);
            config.setMaxTaskParallelism(2);
            config.setDebug(true);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafka", config, KafkaDrpcTrident.buildTopology(drpc));
            while (true) {
                System.out.println("Word count: " + drpc.execute("words", "the"));
                Utils.sleep(1000);
            }
        }
    }


    public static StormTopology buildTopology() {
        return buildTopology(null);
    }

    /**
     * 生成 topology ，根据不同drpc
     *
     * @param drpc 本地传入localDrpc   远程传入null即可
     * @return stormTopology
     */
    public static StormTopology buildTopology(LocalDRPC drpc) {
        // 2.定义拓扑，进行单词统计后，写入一个分布式内存中。
        TridentTopology topology = new TridentTopology();

        // 3.将kafka作为spout 导入 数据，生成一个state 写入内存或者其他地方，方便后面的drpc 客户端进行查询
        TridentState wordCounts = topology.newStream("kaf-impt", kafImp())
                .shuffle()
                .each(new Fields("str"), new WordSplit(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new HazelCastStateFactory(), new Count(), new Fields("aggregates_words"))
                .parallelismHint(2);

        // 4.生成客户端
        topology.newDRPCStream("words", drpc)
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));    //persistentAggregate 和 aggregate   全局batch聚合  局部单个batch聚合
        return topology.build();
    }

    //使用将kafka作为转化成为输入流
    public static OpaqueTridentKafkaSpout kafImp() {
        BrokerHosts zk = new ZkHosts("localhost:2181");
        //1）首先定义一个kafka相关的配置对象，第一个参数是zookeeper的位置，第二个参数是订阅topic的名称，第三个参数是一个clientId
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, "toonew-topic");
        //2）然后对配置进行一些设置，包括一些起始位置之类的，后面再补充具体的配置介绍。
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        //3）创建一个spout，这里的spout是事务型的，也就是保证每一个
        return new OpaqueTridentKafkaSpout(spoutConf);
    }
}

class HazelCastStateFactory implements StateFactory {
    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        return TransactionalMap.build(new HazelCastState(new HazelCastHandler()));
    }
}

class HazelCastHandler implements Serializable {

    private transient Map<String, Long> state;

    public Map<String, Long> getState() {
        if (state == null) {
            state = Hazelcast.newHazelcastInstance().getMap("state");
        }
        return state;
    }
}

class HazelCastState<T> implements IBackingMap<TransactionalValue<Long>> {

    private HazelCastHandler handler;

    public HazelCastState(HazelCastHandler handler) {
        this.handler = handler;
    }

    public void addKeyValue(String key, Long value) {
        Map<String, Long> state = handler.getState();
        state.put(key, value);
    }

    @Override
    public String toString() {
        return handler.getState().toString();
    }


    @Override
    public void multiPut(List<List<Object>> keys, List<TransactionalValue<Long>> vals) {
        for (int i = 0; i < keys.size(); i++) {
            TridentTuple key = (TridentTuple) keys.get(i);
            Long value = vals.get(i).getVal();
            addKeyValue(key.getString(0), value);
            //System.out.println("[" + key.getString(0) + " - " + value + "]");
        }
    }

    public List multiGet(List<List<Object>> keys) {
        List<TransactionalValue<Long>> result = new ArrayList<>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            TridentTuple key = (TridentTuple) keys.get(i);
            result.add(new TransactionalValue<>(0L, MapUtils.getLong(handler.getState(), key.getString(0), 0L)));
        }
        return result;
    }
}
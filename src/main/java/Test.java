import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.testing.TestGlobalCount;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;

/**
 * 最基本的 storm 流程
 * inputStream -->  spout -->  blot
 * 这里只有 输入 和 计算 的操作，怎么获取运算的结果嘞？
 */
public class Test {

    public static void main(String[] args) {
        try {
            //1.创建一个  topology 生成者
            TopologyBuilder builder = new TopologyBuilder();

            //2.设置spout 和 bolt
            builder.setSpout("1", new TestWordSpout(true), 5);
            builder.setSpout("2", new TestWordSpout(true), 3);
            builder.setBolt("3", new TestWordCounter(), 3)
                    .fieldsGrouping("1", new Fields("word"))
                    .fieldsGrouping("2", new Fields("word"));
            builder.setBolt("4", new TestGlobalCount())
                    .globalGrouping("1");

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

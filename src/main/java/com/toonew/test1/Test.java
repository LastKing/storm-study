package com.toonew.test1;

import com.toonew.test1.blot.WordCounter;
import com.toonew.test1.blot.WordNormalizer;
import com.toonew.test1.spout.WordReader;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * 这个类   实现 貌似 是低版本的，看了下文章的时间为2013年 实现的
 * storm 版本为0.6 ，所以错误在所难免
 * http://javanlu.github.io/2013/10/15/getting-started-with-storm-chapter-2/
 */
public class Test {

    public static void main(String[] args) {

        String filePath;
        if (args.length <= 0) {
            String path = System.getProperty("user.dir");
            filePath = path + "/src/main/java/com/toonew/test1/words.txt";
        } else {
            filePath = args[0];
        }

        //Configuration
        Config conf = new Config();
        conf.put("wordsFile", filePath);
        conf.setDebug(true);

        //Topology run
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());
        builder.setBolt("word-normalizer", new WordNormalizer())
                .shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter())
                .shuffleGrouping("word-normalizer");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());

        Utils.sleep(10000);
        cluster.killTopology("Getting-Started-Toplogie");
        cluster.shutdown();

    }

}

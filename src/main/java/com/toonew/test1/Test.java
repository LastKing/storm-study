package com.toonew.test1;

import com.toonew.test1.blot.WordCounter;
import com.toonew.test1.blot.WordNormalizer;
import com.toonew.test1.spout.WordReader;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import java.io.File;

/**
 * 这个类   实现 貌似 是低版本的，看了下文章的时间为2013年 实现的
 * storm 版本为0.6 ，所以错误在所难免
 * http://javanlu.github.io/2013/10/15/getting-started-with-storm-chapter-2/
 */
public class Test {
    public static void main(String[] args) {

        File directory = new File("");//设定为当前文件夹


        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());
        builder.setBolt("word-normalizer", new WordNormalizer())
                .shuffleGrouping("wordreader");
        builder.setBolt("word-counter", new WordCounter())
                .shuffleGrouping("wordnormalizer");

        String filePath;
        if (args[0] == null || "".equals(args[0])) {
            String path = System.getProperty("user.dir");
            filePath = path + "/src/main/java/com/toonew/test1/words.txt";
        } else
            filePath = args[0];

        //Configuration
        Config conf = new Config();
        conf.put("wordsFile", filePath);
        conf.setDebug(false);

        //Topology run
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        cluster.shutdown();
    }
}

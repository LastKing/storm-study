package com.toonew.kafka.blot;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class CountBolt implements IRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> counters;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        counters = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        String str = input.getString(0);

        if (!counters.containsKey(str)) {
            counters.put(str, 1);
        } else {
            Integer count = counters.get(str) + 1;
            counters.put(str, count);
        }
        collector.ack(input);
    }

    @Override
    public void cleanup() {
        for (Map.Entry<String, Integer> entry : counters.entrySet()) {
            System.out.println("blot compute end !");
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}

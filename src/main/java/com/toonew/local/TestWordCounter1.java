package com.toonew.local;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.storm.utils.Utils.tuple;

public class TestWordCounter1 extends BaseBasicBolt {
    public static Logger LOG = LoggerFactory.getLogger(TestWordCounter.class);

    Map<String, Integer> _counts;

    public void prepare(Map stormConf, TopologyContext context) {
        _counts = new HashMap<String, Integer>();
    }

    protected String getTupleValue(Tuple t, int idx) {
        return (String) t.getValues().get(idx);
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = getTupleValue(input, 0);
        int count = 0;
        if (_counts.containsKey(word)) {
            count = _counts.get(word);
        }
        count++;
        _counts.put(word, count);
        collector.emit(tuple(word, count));
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }

}

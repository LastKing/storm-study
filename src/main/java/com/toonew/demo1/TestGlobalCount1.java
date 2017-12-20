package com.toonew.demo1;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TestGlobalCount1 extends BaseRichBolt {
    public static Logger LOG = LoggerFactory.getLogger(TestWordCounter.class);

    private int _count;
    OutputCollector _collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        _count = 0;
    }

    public void execute(Tuple input) {
        LOG.debug("globalCount1 : " + input.getString(0) + " " + _count);
        _count++;
        _collector.emit(input, new Values(_count));
        _collector.ack(input);
    }

    public void cleanup() {

    }

    public Fields getOutputFields() {
        return new Fields("global-count");
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("global-count"));
    }
}

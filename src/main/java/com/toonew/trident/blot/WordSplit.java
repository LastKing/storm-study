package com.toonew.trident.blot;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class WordSplit extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String sentence = tuple.getString(0);

        if (sentence != null) {
            sentence = sentence.replaceAll("\r", "");
            sentence = sentence.replaceAll("\n", "");
            for (String word : sentence.split(" ")) {
                collector.emit(new Values(word));
            }
        }

    }
}

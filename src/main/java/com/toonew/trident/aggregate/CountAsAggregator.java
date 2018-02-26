package com.toonew.trident.aggregate;


import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class CountAsAggregator extends BaseAggregator<CountAsAggregator.State> {

    static class State {
        long count = 0;
    }

    @Override
    public State init(Object batchId, TridentCollector collector) {
        return new State();
    }

    @Override
    public void aggregate(State state, TridentTuple tuple, TridentCollector collector) {
        state.count++;
    }

    @Override
    public void complete(State state, TridentCollector collector) {
        collector.emit(new Values(state.count));
    }

}

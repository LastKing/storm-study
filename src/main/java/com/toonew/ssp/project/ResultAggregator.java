package com.toonew.ssp.project;

import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

public class ResultAggregator implements CombinerAggregator<State> {
    @Override
    public State init(TridentTuple tuple) {
        return (State) tuple.get(0);
    }

    @Override
    public State combine(State val1, State val2) {
        return val1;
    }

    @Override
    public State zero() {
        return null;
    }
}

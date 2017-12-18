package com.toonew.trident;


import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

public class Count implements CombinerAggregator<Long> {

    @Override
    public Long init(TridentTuple tuple) {
        return 1L;
    }

    @Override
    public Long combine(Long val1, Long val2) {
        System.out.println("combine value : " + (val1 + val2));
        return val1 + val2;
    }

    @Override
    public Long zero() {
        return 0L;
    }

}

package com.toonew.ssp.project;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class Split1 extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String str = tuple.getString(0);

        String[] arr = str.split("_");
        collector.emit(new Values("" + arr[0] + "_" + arr[1], arr[2]));
    }
}

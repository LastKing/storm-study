package com.toonew.trident.state;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;

//查询state
public class QueryLocation extends BaseQueryFunction<LocationDB, String> {
    @Override
    public List<String> batchRetrieve(LocationDB state, List<TridentTuple> inputs) {
        List<Long> userIds = new ArrayList<>();
        for (TridentTuple input : inputs) {
            userIds.add(input.getLong(0));
        }
        return state.bulkGetLocations(userIds);
    }

    @Override
    public void execute(TridentTuple tuple, String result, TridentCollector collector) {
        collector.emit(new Values(result));
    }
}
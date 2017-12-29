package com.toonew.trident.state;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.*;
import org.apache.storm.trident.state.map.IBackingMap;
import org.apache.storm.trident.state.map.MapState;
import org.apache.storm.trident.state.map.OpaqueMap;
import org.apache.storm.trident.state.map.SnapshottableMap;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryMapState<T> implements MapState<T> {

    MemoryMapState.MemoryMapStateBacking<OpaqueValue> _backing;
    SnapshottableMap<T> _delegate;

    public MemoryMapState(String id) {
        this._backing = new MemoryMapStateBacking<>(id);
        this._delegate = new SnapshottableMap<>(OpaqueMap.build(_backing), new Values("$MEMORY-MAP-STATE-GLOBAL$"));
    }

    //*****  MapState 的实现报错 state 和 批量任务操作
    @Override
    public void beginCommit(Long txid) {

    }

    @Override
    public void commit(Long txid) {

    }

    @Override
    public List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater> updaters) {
        return null;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {

    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        return null;
    }


    //***** state工厂
    public static class Factory implements StateFactory {

        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            return null;
        }
    }


    static ConcurrentHashMap<String, Map<List<Object>, Object>> _dbs = new ConcurrentHashMap<>();

    //***** 工厂生成
    static class MemoryMapStateBacking<T> implements IBackingMap<T>, ITupleCollection {
        Map<List<Object>, T> db;

        public MemoryMapStateBacking(String id) {
            if (!_dbs.containsKey(id)) {
                _dbs.put(id, new HashMap<>());
            }
            this.db = (Map<List<Object>, T>) _dbs.get(id);
        }

        @Override
        public List<T> multiGet(List<List<Object>> keys) {
            return null;
        }

        @Override
        public void multiPut(List<List<Object>> keys, List<T> vals) {

        }

        @Override
        public Iterator<List<Object>> getTuples() {
            return null;
        }
    }
}

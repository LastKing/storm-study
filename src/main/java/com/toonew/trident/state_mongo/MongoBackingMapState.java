package com.toonew.trident.state_mongo;

import com.google.common.collect.Maps;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.toonew.db.MongoSspUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.*;
import org.apache.storm.trident.state.map.*;
import org.apache.storm.trident.state.snapshot.Snapshottable;
import org.apache.storm.tuple.Values;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MongoBackingMapState<T> implements Snapshottable<T>, ITupleCollection, MapState<T>, RemovableMapState<T> {
    private static final Logger LOG = LoggerFactory.getLogger(MongoBackingMapState.class);

    public static class Options<T> implements Serializable {
        private static final long serialVersionUID = 1L;
        public Serializer<T> serializer;
        public String globalKey = "$MONGO-MAP-STATE-GLOBAL$";
    }

    SspMapStateBacking<OpaqueValue> _backing;
    SnapshottableMap<T> _delegate;
    List<List<Object>> _removed = new ArrayList<>();
    Long _currTx = null;

    public MongoBackingMapState(final Options<T> options, Map conf, int partitionNum) {
        _backing = new SspMapStateBacking(options, conf, partitionNum);
        _delegate = new SnapshottableMap(OpaqueMap.build(_backing), new Values(options.globalKey));
    }

    @Override
    public Iterator<List<Object>> getTuples() {
        return _backing.getTuples();
    }

    @Override
    public List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater> updaters) {
        return _delegate.multiUpdate(keys, updaters);
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        _delegate.multiPut(keys, vals);
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        return _delegate.multiGet(keys);
    }

    @Override
    public void multiRemove(List<List<Object>> keys) {
        List nulls = new ArrayList();
        for (int i = 0; i < keys.size(); i++) {
            nulls.add(null);
        }
        // first just set the keys to null, then flag to remove them at beginning of next commit when we know the current and last value are both null
        multiPut(keys, nulls);
        _removed.addAll(keys);
    }

    @Override
    public T update(ValueUpdater updater) {
        return _delegate.update(updater);
    }

    @Override
    public void set(T o) {
        _delegate.set(o);
    }

    @Override
    public T get() {
        return _delegate.get();
    }

    @Override
    public void beginCommit(Long txid) {
        _delegate.beginCommit(txid);
        if (txid == null || !txid.equals(_currTx)) {
            _backing.multiRemove(_removed);
        }
        _removed = new ArrayList<>();
        _currTx = txid;
    }

    @Override
    public void commit(Long txid) {
        _delegate.commit(txid);
    }


    public static class Factory implements StateFactory {
        private static final long serialVersionUID = 1L;
        private Options<OpaqueValue> options = new Options<>();

        private static final Map<StateType, Serializer> DEFAULT_SERIALIZERS = Maps.newHashMap();

        static {
            DEFAULT_SERIALIZERS.put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
            DEFAULT_SERIALIZERS.put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
            DEFAULT_SERIALIZERS.put(StateType.OPAQUE, new JSONOpaqueSerializer());
        }


        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            if (this.options.serializer == null) {
                this.options.serializer = DEFAULT_SERIALIZERS.get(StateType.OPAQUE);
            }
            return new MongoBackingMapState(this.options, conf, partitionIndex);
        }
    }

    static class SspMapStateBacking<T> implements IBackingMap<T>, ITupleCollection {
        private Serializer<T> serializer;
        private int partitionNum;
        static MongoCollection<Document> mongo_Client_coll;

        public SspMapStateBacking(final Options<T> options, Map conf, int partitionNum) {
            this.serializer = options.serializer;
            this.partitionNum = partitionNum;
            mongo_Client_coll = MongoSspUtil.instance.getCollection("ssp_compute", "imp_counter");
        }

        @Override
        public Iterator<List<Object>> getTuples() {
            return null;
        }

        public void multiRemove(List<List<Object>> keys) {
            for (List<Object> key : keys) {
//                db.remove(key);
            }
        }

        @Override
        public List<T> multiGet(List<List<Object>> keys) {
            List<T> retval = new ArrayList<>();
            byte[] ObjId, de_mon;
            String str_objid;
            Document value;
            Document doc = new Document();

            for (List<Object> key : keys) {
                doc.clear();
                ObjId = getObjectId(key);
                str_objid = Base64.encodeBase64String(ObjId);       //将对象解析为base64
                doc.put("_id", str_objid);
                value = mongo_Client_coll.find(doc).first();

                if (value != null) {
                    de_mon = Base64.decodeBase64(value.getString("value"));
                    retval.add(this.serializer.deserialize(de_mon));
                } else {
                    retval.add(null);
                }
            }

            return retval;
        }

        @Override
        public void multiPut(List<List<Object>> keys, List<T> vals) {
            byte[] ObjId, ser_dev;
            Bson filter;
            String tt_base64;

            UpdateOptions options = new UpdateOptions();
            options.upsert(true);//mongo中如果有数据就更新，没有就insert

            Document doc = new Document();

            for (int i = 0; i < keys.size(); i++) {
                LOG.info("Persisting [" + keys.get(i).get(0) + "] ==> [" + vals.get(i) + "]");
                System.out.println("Persisting [" + keys.get(i).get(0) + "] ==> [" + vals.get(i) + "]");

                doc.clear();
                ObjId = getObjectId(keys.get(i)); //获取保存在mongo中的唯一ID
                filter = Filters.eq("_id", Base64.encodeBase64String(ObjId));//获取要替换的ID

                ser_dev = this.serializer.serialize(vals.get(i));
                tt_base64 = Base64.encodeBase64String(ser_dev);
                doc.append("_id", Base64.encodeBase64String(ObjId));
                doc.append("name", keys.get(i).get(0).toString());
                doc.append("value", tt_base64);
                mongo_Client_coll.replaceOne(filter, doc, options);
            }
        }

    }

    //通过key获取唯一id
    public static byte[] getObjectId(List<Object> keys) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            for (Object key : keys) {
                bos.write(String.valueOf(key).getBytes());
            }
            bos.close();
        } catch (IOException e) {
            throw new RuntimeException("IOException creating Mongo document _id.", e);
        }
        return bos.toByteArray();
    }
}

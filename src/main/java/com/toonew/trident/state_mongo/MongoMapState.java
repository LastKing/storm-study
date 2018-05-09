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
import org.apache.storm.tuple.Values;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MongoMapState<T> implements IBackingMap<T> {
    private static final Logger LOG = LoggerFactory.getLogger(MongoMapState.class);

    static MongoCollection<Document> mongo_Client_coll;

    private Serializer<T> serializer;

    private static final Map<StateType, Serializer> DEFAULT_SERIALIZERS = Maps.newHashMap();

    static {
        DEFAULT_SERIALIZERS.put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
        DEFAULT_SERIALIZERS.put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
        DEFAULT_SERIALIZERS.put(StateType.OPAQUE, new JSONOpaqueSerializer());
    }

    public MongoMapState(final Options<T> options, Map conf) {
        this.serializer = options.serializer;
        mongo_Client_coll = MongoSspUtil.instance.getCollection("mongo_compute", "imp_counter");
    }

    public static class Options<T> implements Serializable {
        private static final long serialVersionUID = 1L;
        public Serializer<T> serializer;
        public String globalKey = "$MONGO-MAP-STATE-GLOBAL$";
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        List<T> retval = new ArrayList<>();
        byte[] ObjId;
        String str_objId;
        Document value;
        Document doc = new Document();

        for (List<Object> key : keys) {
            doc.clear();
            ObjId = getObjectId(key);
            str_objId = Base64.encodeBase64String(ObjId);       //将对象解析为base64
            doc.put("_id", str_objId);
            value = mongo_Client_coll.find(doc).first();

            if (value != null)
                retval.add(this.serializer.deserialize(value.getString("value").getBytes()));
            else
                retval.add(null);
        }

        return retval;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        byte[] ObjId;
        Bson filter;

        UpdateOptions options = new UpdateOptions();
        options.upsert(true);//mongo中如果有数据就更新，没有就insert

        Document doc = new Document();

        for (int i = 0; i < keys.size(); i++) {
            LOG.info("Persisting [" + keys.get(i).get(0) + "] ==> [" + vals.get(i) + "]");
            System.out.println("Persisting [" + keys.get(i).get(0) + "] ==> [" + vals.get(i) + "]");

            doc.clear();
            ObjId = getObjectId(keys.get(i)); //获取保存在mongo中的唯一ID
            filter = Filters.eq("_id", Base64.encodeBase64String(ObjId));//获取要替换的ID

            String valueSer = new String(this.serializer.serialize(vals.get(i)));
            doc.append("_id", Base64.encodeBase64String(ObjId));
            doc.append("name", keys.get(i).get(0).toString());
            doc.append("value", valueSer);
            mongo_Client_coll.replaceOne(filter, doc, options);
        }
    }

    //    @Override
//    public List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater> updaters) {
//        List<CachedBatchReadsMap.RetVal<OpaqueValue>> curr = _backing.multiGet(keys);
//        List<OpaqueValue> newVals = new ArrayList<OpaqueValue>(curr.size());
//        List<T> ret = new ArrayList<T>();
//        for(int i=0; i<curr.size(); i++) {
//            CachedBatchReadsMap.RetVal<OpaqueValue> retval = curr.get(i);
//            OpaqueValue<T> val = retval.val;
//            ValueUpdater<T> updater = updaters.get(i);
//            T prev;
//            if(val==null) {
//                prev = null;
//            } else {
//                if(retval.cached) {
//                    prev = val.getCurr();
//                } else {
//                    prev = val.get(_currTx);
//                }
//            }
//            T newVal = updater.update(prev);
//            ret.add(newVal);
//            OpaqueValue<T> newOpaqueVal;
//            if(val==null) {
//                newOpaqueVal = new OpaqueValue<T>(_currTx, newVal);
//            } else {
//                newOpaqueVal = val.update(_currTx, newVal);
//            }
//            newVals.add(newOpaqueVal);
//        }
//        _backing.multiPut(keys, newVals);
//        return ret;
//    }


    /**
     * 生成并返回一个  可操作的Map
     */
    public static class Factory implements StateFactory {

        private static final long serialVersionUID = 1L;

        private Options<OpaqueValue> options = new Options<>();

        public Factory() {
            if (this.options.serializer == null) {
                this.options.serializer = DEFAULT_SERIALIZERS.get(StateType.OPAQUE);
            }
        }

        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            IBackingMap state = new MongoMapState(this.options, conf);

            state = new CachedMap(state, 5000);

            MapState mapState = OpaqueMap.build(state);

            return new SnapshottableMap(mapState, new Values(this.options.globalKey));
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

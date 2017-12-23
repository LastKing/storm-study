package com.toonew.trident.state;

import org.apache.storm.trident.state.State;

import java.util.ArrayList;
import java.util.List;

/**
 * 详解 trident 中 state
 * http://blog.csdn.net/suifeng3051/article/details/49332353
 */
public class LocationDB implements State {

    @Override
    public void beginCommit(Long txid) {

    }

    @Override
    public void commit(Long txid) {

    }

    //设置批量 Location 数据
    public void setLocationsBulk(List<Long> userIds, List<String> locations) {
        // set locations in bulk
    }

    public List<String> bulkGetLocations(List<Long> userIds) {
        // get locations in bulk
        List<String> list = new ArrayList<>();

        return list;
    }


    //设置单个 Location 数据
    public void setLocation(long userId, String location) {
        // code to access database and set location
    }

    public String getLocation(long userId) {
        // code to get location from database
        return "" + userId;
    }
}

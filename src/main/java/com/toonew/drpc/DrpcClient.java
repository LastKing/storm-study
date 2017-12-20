package com.toonew.drpc;

import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;

/**
 * 查询远程storm服务中的topology中的数据
 */
public class DrpcClient {

    public static void main(String[] args) throws Exception {
        //1.创建drpc client  基本配置 和 remote [ host  port ]
        DRPCClient client = new DRPCClient(Utils.readDefaultConfig(), "127.0.0.1", 3772);
        // prints the JSON-encoded result, e.g.: "[[5078]]"
        System.out.println(client.execute("words", "cat dog the man"));//远程drpc function name ，query word
    }

}

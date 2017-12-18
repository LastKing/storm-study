package com.toonew.trident;

import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;

public class drpc {
    public static void main(String[] args) throws Exception {
        DRPCClient client = new DRPCClient(Utils.readDefaultConfig(), "127.0.0.1", 3772);
        // prints the JSON-encoded result, e.g.: "[[5078]]"
        System.out.println(client.execute("words", "cat dog the man"));
    }
}

package com.toonew.custom;

import org.apache.storm.topology.TopologyBuilder;

public class Toonew {

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("1", new ToonewSpout(true), 5);

    }

}

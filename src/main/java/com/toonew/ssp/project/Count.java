package com.toonew.ssp.project;

import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

import java.io.Serializable;

class State implements Serializable {
    private String str;
    private double total;
    private long count;

    public State(String str, double total, long count) {
        this.str = str;
        this.total = total;
        this.count = count;
    }

    public String getStr() {
        return str;
    }

    public void setStr(String str) {
        this.str = str;
    }

    public double getTotal() {
        return total;
    }

    public void setTotal(double total) {
        this.total = total;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "\"State{" +
                "str='" + str + '\'' +
                ", total=" + total +
                ", count=" + count +
                "}\"";
    }
}

public class Count implements CombinerAggregator<State> {
    @Override
    public State init(TridentTuple tuple) {
        String string = tuple.getString(0);
        Double price = Double.parseDouble(tuple.getString(1));

        return new State(string, price, 0);
    }

    //注意返回val1 对 val2 透明事务的影响
    @Override
    public State combine(State val1, State val2) {
        if ("".equals(val1.getStr()))
            val1.setStr(val2.getStr());

        val2.setTotal(val1.getTotal() + val2.getTotal());
        val2.setCount(val1.getCount() + 1);
        return val2;
    }

    @Override
    public State zero() {
        return new State("", 0, 0);
    }
}

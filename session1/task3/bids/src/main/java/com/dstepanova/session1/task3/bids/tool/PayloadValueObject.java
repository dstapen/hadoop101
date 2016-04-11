package com.dstepanova.session1.task3.bids.tool;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public final class PayloadValueObject implements Writable {
    private LongWritable visitCount = new LongWritable();
    private LongWritable spendMoney = new LongWritable();

    public LongWritable getVisitCount() {
        return visitCount;
    }

    public void setVisitCount(LongWritable visitCount) {
        this.visitCount = visitCount;
    }

    public LongWritable getSpendMoney() {
        return spendMoney;
    }

    public void setSpendMoney(LongWritable spendMoney) {
        this.spendMoney = spendMoney;
    }

    @Override
    public void write(DataOutput aDataOutput) throws IOException {
        visitCount.write(aDataOutput);
        spendMoney.write(aDataOutput);
    }

    @Override
    public void readFields(DataInput aDataInput) throws IOException {
        visitCount.readFields(aDataInput);
        spendMoney.readFields(aDataInput);
    }

    @Override
    public String toString() {
        return "PayloadValueObject{" +
                "visitCount=" + visitCount +
                ", spendMoney=" + spendMoney +
                '}';
    }
}

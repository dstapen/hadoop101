package com.dstepanova.session1.task3.bids.tool;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public final class ManagedReducer extends Reducer<Text, PayloadValueObject, Text, PayloadValueObject> {
    @Override
    protected void reduce(Text key, Iterable<PayloadValueObject> values, Context context) throws IOException, InterruptedException {
        PayloadValueObject sum = new PayloadValueObject();
        for(PayloadValueObject bidding: values) {
            long visits = sum.getVisitCount().get() + bidding.getVisitCount().get();
            sum.setVisitCount(new LongWritable(visits));
            long spends = sum.getSpendMoney().get() + bidding.getSpendMoney().get();
            sum.setSpendMoney(new LongWritable(spends));
        }
        context.write(key, sum);
    }

}

package com.dstepanova.session1.task4.tool;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ManagedReducer extends Reducer<CompositeKey, Text, Text, NullWritable> {

    private NullWritable outValue = NullWritable.get();

    @Override
    protected void reduce(CompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Counter counter = context.getCounter("MaxIPinYouId", key.getiPinyouId().toString());

        if(key.getStreamId().get() == 1) {
            counter.increment(1);
        }

        for(Text value: values) {
            context.write(value, outValue);
        }
    }
}
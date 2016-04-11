package com.dstepanova.session1.task3.tags.tool;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class ManagedReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(ManagedReducer.class);

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        final long tally = StreamSupport.stream(values.spliterator(), false)
                .mapToLong(LongWritable::get)
                .sum();

        if (true){
            LOG.info("staging tally = {}", tally);
        }

        final LongWritable value = new LongWritable();
        value.set(tally);
        context.write(key, value);
    }
}

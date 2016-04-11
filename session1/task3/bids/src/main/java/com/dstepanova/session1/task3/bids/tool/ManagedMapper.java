package com.dstepanova.session1.task3.bids.tool;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class ManagedMapper extends Mapper<LongWritable, Text, Text, PayloadValueObject> {
    private static final Logger LOG = LoggerFactory.getLogger(ManagedMapper.class);

    private Helper helper = new Helper();

    private Text ip = new Text();
    private PayloadValueObject biddings = new PayloadValueObject();

    public ManagedMapper() {
        biddings.getVisitCount().set(1);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        helper.extract(value.toString());
        if(helper.isFailed()) {
            LOG.warn("Could not extract line.");
            return;
        }
        context.getCounter("Browser Group", helper.getBrowser()).increment(1);
        ip.set(helper.getIp());
        biddings.getSpendMoney().set(helper.getBidings());
        context.write(ip, biddings);
    }
}

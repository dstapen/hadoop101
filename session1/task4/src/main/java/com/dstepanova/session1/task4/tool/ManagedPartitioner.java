package com.dstepanova.session1.task4.tool;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ManagedPartitioner extends Partitioner<CompositeKey, Text> {


    @Override
    public int getPartition(CompositeKey compositeKey, Text value, int numPartitions) {
        return (compositeKey.getiPinyouId().hashCode() * 13) % numPartitions;
    }
}

package com.dstepanova.session1.task3.tags.tool;


import org.junit.Test;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;


import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

public class ManagedReducerTest {
    final ManagedReducer sut = new ManagedReducer();
    final ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver = ReduceDriver.newReduceDriver(sut);

    @Test
    public void testReduce() throws Exception {
        reduceDriver.withInput(new Text("one"), Collections.singletonList(new LongWritable(1)));
        reduceDriver.withInput(new Text("two"), Arrays.asList(new LongWritable(1), new LongWritable(1)));
        List<Pair<Text, LongWritable>> result = reduceDriver.run();
        assertThat(result.size(), equalTo(2));
        assertThat(result.get(0).getFirst().toString(), equalTo("one"));
        assertThat(result.get(0).getSecond().get(), equalTo(1L));
        assertThat(result.get(1).getFirst().toString(), equalTo("two"));
        assertThat(result.get(1).getSecond().get(), equalTo(2L));
    }
}
package com.dstepanova.session1.task3.bids.tool;

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
    final ReduceDriver<Text, PayloadValueObject, Text, PayloadValueObject> reduceDriver = ReduceDriver.newReduceDriver(sut);

    @Test
    public void testReduce() throws Exception {
        PayloadValueObject foo = new PayloadValueObject();
        foo.setVisitCount(new LongWritable(1L));
        foo.setSpendMoney(new LongWritable(10L));

        PayloadValueObject bar = new PayloadValueObject();
        bar.setVisitCount(new LongWritable(1L));
        bar.setSpendMoney(new LongWritable(20L));

        PayloadValueObject baz = new PayloadValueObject();
        baz.setVisitCount(new LongWritable(1L));
        baz.setSpendMoney(new LongWritable(30L));


        List<PayloadValueObject> bytesList = Arrays.asList(
                foo,
                bar,
                baz
        );
        reduceDriver.withInput(new Text("118.254.16.*"), bytesList);

        List<Pair<Text, PayloadValueObject>> result = reduceDriver.run();

        assertThat(result.size(), equalTo(1));

        Text key = result.get(0).getFirst();
        assertThat(key.toString(), equalTo("118.254.16.*"));

        PayloadValueObject value = result.get(0).getSecond();
        assertThat(value.getVisitCount().get(), equalTo(3L));
        assertThat(value.getSpendMoney().get(), equalTo(60L));
    }
}
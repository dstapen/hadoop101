package com.dstepanova.session1.task4.tool;

import com.google.common.io.Resources;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.mrunit.mapreduce.ReduceDriver.newReduceDriver;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class ManagedReducerTest {
    private final ManagedReducer sut = new ManagedReducer();
    private final ReduceDriver aReduceDriver = newReduceDriver(sut);

    @Test
    public void testReduce() throws Exception {
        URL url = Resources.getResource("com/dstepanova/session1/task4/tool/input_reducer.txt");
        String text = Resources.toString(url, StandardCharsets.UTF_8);

        CompositeKey key = new CompositeKey();
        key.getiPinyouId().set("asdqwe");
        key.getTimestamp().set(113);
        key.getStreamId().set(1);

        List<Text> values = Arrays.asList(new Text(text));

        aReduceDriver.withInput(key, values);

        List<Pair<Text, NullWritable>> result = aReduceDriver.run();

        Counter counter = aReduceDriver.getCounters().getGroup("MaxIPinYouId").iterator().next();
        assertThat(counter.getName(), equalTo("asdqwe"));
        assertThat(counter.getValue(), equalTo(1L));
    }
}
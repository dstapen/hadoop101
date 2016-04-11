package com.dstepanova.session1.task3.bids.tool;

import com.google.common.io.Resources;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.apache.hadoop.mrunit.mapreduce.MapDriver.newMapDriver;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ManagedMapperTest {
    private final ManagedMapper sut = new ManagedMapper();
    private final MapDriver<LongWritable, Text, Text, PayloadValueObject> aMapDriver = newMapDriver(sut);

    @Test
    public void testMap() throws Exception {
        URL url = Resources.getResource("com/dstepanova/session1/task3/input_mapper.txt");
        String text = Resources.toString(url, StandardCharsets.UTF_8);

        aMapDriver.withInput(new LongWritable(10), new Text(text));
        List<Pair<Text, PayloadValueObject>> result = aMapDriver.run();
        Text actualKey = result.get(0).getFirst();
        PayloadValueObject actualValue = result.get(0).getSecond();

        assertThat(2489L, equalTo(actualValue.getSpendMoney().get()));
        assertThat(1L, equalTo(actualValue.getVisitCount().get()));
        assertEquals(1, result.size());
        assertThat(actualKey.toString(), equalTo("10.242.168.*"));
    }
}
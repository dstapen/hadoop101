package com.dstepanova.session1.task3.tags.tool;

import com.google.common.io.Resources;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static java.lang.String.format;
import static org.apache.hadoop.mrunit.mapreduce.MapDriver.newMapDriver;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public final class ManagedMapperTest {

    private final ManagedMapper sut = new ManagedMapper();
    private final MapDriver<LongWritable, Text, Text, LongWritable> aMapDriver = newMapDriver(sut);

    @Before
    public void beforeEachTestAddCache() throws URISyntaxException {
        aMapDriver.addCacheFile(ManagedMapperTest.class.getResource("cache_one_line.txt").toURI());
    }

    @Test
    public void testMap() throws Exception {

        URL url = Resources.getResource("com/dstepanova/session1/task3/tags/tool/input.txt");
        String text = Resources.toString(url, StandardCharsets.UTF_8);
        aMapDriver.withInput(new LongWritable(10), new Text(text));

        List<Pair<Text, LongWritable>> result = aMapDriver.run();


        assertEquals(3, result.size());

        assertThat(result.get(0).getFirst().toString(), equalTo("one"));
        assertThat(result.get(0).getSecond().get(), equalTo(1L));

        assertThat(result.get(1).getFirst().toString(), equalTo("two"));
        assertThat(result.get(1).getSecond().get(), equalTo(1L));

        assertThat(result.get(2).getFirst().toString(), equalTo("two"));
        assertThat(result.get(2).getSecond().get(), equalTo(1L));
    }
}
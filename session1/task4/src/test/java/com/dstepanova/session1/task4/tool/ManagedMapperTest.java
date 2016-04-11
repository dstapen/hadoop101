package com.dstepanova.session1.task4.tool;

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
import static org.junit.Assert.*;

public class ManagedMapperTest {
    private final ManagedMapper sut = new ManagedMapper();
    private final MapDriver<LongWritable, Text, CompositeKey, Text> aMapDriver = newMapDriver(sut);
//    private static final String BIDDINGS_TEMPLATE = "11baa543d120063f0f161b54232c7202\t" +
//            "%s\t" + // <-- Time
//            "%s\t" + //
//            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)\t" +
//            "123.123.123.123\t" +
//            "201\t" +
//            "209\t" +
//            "3\t" +
//            "3KFal19xGq1m1YdI5SqfNX\t" +
//            "1df08a1077dbcc6b147b6b2a0889f999\t" +
//            "null\t" +
//            "Digital_F_Width1\t" +
//            "1000\t" +
//            "90\t" +
//            "0\t" +
//            "0\t" +
//            "31\t" +
//            "c46090c887c257b61ab1fa11baee91d8\t" +
//            "120\t" +
//            "3427\t" +
//            "282825712806\t" +
//            "1";


    @Test
    public void testMap() throws Exception {
        URL url = Resources.getResource("com/dstepanova/session1/task4/tool/input_mapper.txt");
        String text = Resources.toString(url, StandardCharsets.UTF_8);

        Text line = new Text(text);
        aMapDriver.withInput(new LongWritable(10), line);
        List<Pair<CompositeKey, Text>> result = aMapDriver.run();
        CompositeKey actualKey = result.get(0).getFirst();
        Text actualValue = result.get(0).getSecond();

        assertEquals(1, result.size());
        assertThat(actualKey.getiPinyouId().toString(), equalTo("asdqwe"));
        assertThat(actualKey.getTimestamp().get(), equalTo(113L));
        assertThat(actualValue, equalTo(line));
    }
}
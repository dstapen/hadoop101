package com.dstepanova.session2.task2;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantStringObjectInspector;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class UserAgentFuncTest {

    @Test
    public void ensureDefaultConstructorExistence() throws Exception {
        UserAgentFunc uaf = UserAgentFunc.class.newInstance();
        assertNotNull(uaf);
    }

    @Test
    @Ignore
    public void test() throws Exception {
        UserAgentFunc sut = UserAgentFunc.class.newInstance();
        JavaConstantStringObjectInspector jsoi = new JavaConstantStringObjectInspector("Mozilla/5.0 (iPad; U; CPU OS 3_2_1 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, like Gecko) Mobile/7B405");
        StructObjectInspector result = sut.initialize(new ObjectInspector[]{jsoi});
        System.out.println(
                result.getStructFieldRef("browser_type").getFieldObjectInspector().toString());

    }



}
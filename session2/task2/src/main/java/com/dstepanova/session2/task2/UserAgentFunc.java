
package com.dstepanova.session2.task2;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.OperatingSystem;
import eu.bitwalker.useragentutils.UserAgent;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Strings.isNullOrEmpty;

public class UserAgentFunc extends GenericUDTF {

    private PrimitiveObjectInspector stringOI = null;


    private PrimitiveObjectInspector validate(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if(objectInspectors.length > 1) {
            throw new UDFArgumentException("accept one parameter only");
        }

        ObjectInspector returnValue = objectInspectors[0];
        if (returnValue.getCategory() != ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) returnValue).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("parameter must be string");
        }
        return (PrimitiveObjectInspector) returnValue;
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        stringOI = validate(objectInspectors);

        List<String> fieldNames = new ArrayList<String>(4) {{
            add("browser_type");
            add("family");
            add("os");
            add("device_type");
        }};
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(4) {{
            add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }};
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        final String userAgentString = stringOI.getPrimitiveJavaObject(objects[0]).toString();

        if (isNullOrEmpty(userAgentString)) {
            return;
        }

        UserAgent ua = new UserAgent(userAgentString);
        Browser browser = ua.getBrowser();
        OperatingSystem os = ua.getOperatingSystem();

        forward(new Object[] { browser.getBrowserType().getName(), browser.getGroup().name(), os.getName(), os.getDeviceType().getName()});
    }

    @Override
    public void close() {
        // do nothing
    }
}
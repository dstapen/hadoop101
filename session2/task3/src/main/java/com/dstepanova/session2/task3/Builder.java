package com.dstepanova.session2.task3;


import org.apache.flume.Context;
import org.apache.flume.interceptor.Interceptor;

public class Builder implements Interceptor.Builder {

    private String path;

    public void configure(Context context) {
        path = context.getString("path");
    }

    public Interceptor build() {
        return new FlumeInterceptor(path);
    }
}

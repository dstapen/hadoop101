package com.dstepanova.session2.task3;

import org.apache.flume.Context;
import org.apache.flume.interceptor.Interceptor;

/**
 * there is no entry point. it's dedicated for debugging only.
 */
public interface App {

    default void main(String[] args) {
        Builder builder = new Builder();
        builder.configure(new Context());
        Interceptor aFlumeInterceptor = builder.build();

    }
}

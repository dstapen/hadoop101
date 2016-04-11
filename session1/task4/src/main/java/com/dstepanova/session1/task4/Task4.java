package com.dstepanova.session1.task4;

import com.dstepanova.session1.task4.tool.ConfiguredTool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;

import java.io.File;

import static org.slf4j.LoggerFactory.getLogger;

public final class Task4 implements Runnable, AutoCloseable {
    private static final Logger LOG = getLogger(Task4.class);
    private final ConfiguredTool aTool;
    private final String[] args;

    public Task4(ConfiguredTool aTool, Context aContext) {
        this.aTool = aTool;
        this.args = aContext.toArguments();
    }

    public static void main(String[] args) {
        try (final Task4 app = build()) {
            app.run();
        } catch (StateException e){
            LOG.info("ERROR: {}\nUSAGE: yarn -Dsrc=... -Dout=... jar {}", e.getMessage(), grabJavaArchive());
        } catch (Exception e) {
            LOG.error("FATAL error", e);
        }
    }

    @Override
    public void run() {
        LOG.info("Application is up and running.");
        try {
            ToolRunner.run(aTool, args);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("Application is shutting down");
    }

    private static Task4 build() {
        LOG.info("Application is initializing...");
        return new Task4(new ConfiguredTool(), new Context());
    }

    static String grabJavaArchive() {
        return new File(Context.class.getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .getPath())
                .getName();
    }

}

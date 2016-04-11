package com.dstepanova.session1.task3.tags;

import com.dstepanova.session1.task3.StateException;
import com.dstepanova.session1.task3.tags.tool.ConfiguredTool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;

import static com.dstepanova.session1.task3.tags.Context.grabJavaArchive;
import static org.slf4j.LoggerFactory.getLogger;

public class Tags implements Runnable, AutoCloseable {
    private static final Logger LOG = getLogger(Tags.class);
    private final ConfiguredTool aTool;
    private final String[] args;

    public Tags(ConfiguredTool aTool, Context aContext) {
        this.aTool = aTool;
        this.args = aContext.toArguments();
    }

    public static void main(String[] args) {
        try (final Tags app = build()) {
            app.run();
        } catch (StateException e){
            LOG.info("ERROR: {}\nUSAGE: yarn -Dsrc=... -Dout=... -Denrich=... jar {}",
                    e.getMessage(), grabJavaArchive());
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

    private static Tags build() {
        LOG.info("Application is initializing...");
        return new Tags(new ConfiguredTool(), new Context());
    }
}

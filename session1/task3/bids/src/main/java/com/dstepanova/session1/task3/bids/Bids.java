package com.dstepanova.session1.task3.bids;


import com.dstepanova.session1.task3.StateException;
import com.dstepanova.session1.task3.bids.tool.ConfiguredTool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;

import static com.dstepanova.session1.task3.bids.Context.grabJavaArchive;
import static org.slf4j.LoggerFactory.getLogger;

public final class Bids implements Runnable, AutoCloseable {
    private static final Logger LOG = getLogger(Bids.class);
    private final ConfiguredTool aTool;
    private final String[] args;

    public static void main(final String[] args) {
        try (final Bids app = build()) {
            app.run();
        } catch (StateException e){
            LOG.info("ERROR: {}\nUSAGE: yarn -Dsrc=... -Dout=... jar {}",
                    e.getMessage(), grabJavaArchive());
        } catch (Exception e) {
            LOG.error("FATAL error", e);
        }
    }

    public Bids(Context aContext, ConfiguredTool aTool) {
        this.args = aContext.toArguments();
        this.aTool = aTool;
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

    private static Bids build() {
        LOG.info("Application is initializing...");
        return new Bids(new Context(), new ConfiguredTool());
    }
}

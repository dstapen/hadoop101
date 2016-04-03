package com.dstepanova.session1.task2.master;

import org.slf4j.Logger;
import static org.slf4j.LoggerFactory.getLogger;
import static com.dstepanova.session1.task2.master.MasterApp.builder;

public class MasterLauncher {
    private static final Logger LOGGER = getLogger(MasterLauncher.class);

    public static void main(String[] args) {
        try (MasterApp app = builder().build()) {
            app.run();
        } catch (Exception e) {
            LOGGER.error("Abnormal application termination.", e);
        }
    }

}

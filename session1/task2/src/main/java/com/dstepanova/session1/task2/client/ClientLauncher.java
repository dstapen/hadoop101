package com.dstepanova.session1.task2.client;

import com.dstepanova.session1.task2.master.MasterLauncher;
import org.slf4j.Logger;

import static com.dstepanova.session1.task2.client.ClientApp.builder;
import static org.slf4j.LoggerFactory.getLogger;

public final class ClientLauncher {
    private static final Logger LOG = getLogger(ClientLauncher.class);

    public static void main(String[] args) {
        try (ClientApp app = builder().forClass(MasterLauncher.class).build()) {
            app.run();
        } catch (Exception e) {
            LOG.error("Abnormal application termination.", e);
        }
    }
}

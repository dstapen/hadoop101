package com.dstepanova.session1.task2.client;

import com.dstepanova.session1.task2.StateException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.dstepanova.session1.task2.Requirements.requireNotNullArgument;
import static java.util.Optional.ofNullable;
import static org.slf4j.LoggerFactory.getLogger;

@ThreadSafe
public class ManagedMonitor implements Runnable {
    private static final Logger LOG = getLogger(ManagedMonitor.class);
    private final ScheduledExecutorService aScheduler;
    private final ApplicationId anApplicationId;
    private final YarnClient anYarnClient;
    private volatile ScheduledFuture<?> aScheduledFuture;


    public ManagedMonitor(YarnClient anYarnClient, ApplicationId anApplicationId) {
        this(Executors.newScheduledThreadPool(1), anYarnClient, anApplicationId);
    }

    public ManagedMonitor(final ScheduledExecutorService aScheduler,
                          final YarnClient anYarnClient,
                          final ApplicationId anApplicationId) {
        this.aScheduler = requireNotNullArgument(aScheduler, "scheduler service must not be null.");
        this.anYarnClient = requireNotNullArgument(anYarnClient, "Yarn client must not be null.");
        this.anApplicationId = requireNotNullArgument(anApplicationId, "Application idenitity must not be null");
    }

    public ScheduledFuture<?> start(long delay, TimeUnit unit) {
        synchronized (this) {
            aScheduledFuture = aScheduler.scheduleAtFixedRate(this, delay, delay, unit);
        }
        return aScheduledFuture;
    }

    public void stop() {
        synchronized (this) {
            if (aScheduledFuture != null) {
                aScheduledFuture.cancel(true);
                aScheduledFuture = null;
            }
        }
    }

    public void await() {
        try {
            aScheduledFuture.get();
        } catch (InterruptedException e) {
            LOG.error("spurious wake up", e);
            Thread.currentThread().interrupt();
        } catch (CancellationException e) {
            // skip it
        } catch (ExecutionException e) {
            throw new StateException(e);
        }
    }

    @Override
    public void run() {
        ofNullable(getApplicationReport()).ifPresent((report) -> {

            LOG.info("Got application report from ASM for"
                    + ", appId=" + anApplicationId.getId()
                    + ", clientToAMToken=" + report.getClientToAMToken()
                    + ", diagnostics=" + report.getDiagnostics()
                    + ", host=" + report.getHost()
                    + ", queue=" + report.getQueue()
                    + ", rpcPort=" + report.getRpcPort()
                    + ", startTime=" + report.getStartTime()
                    + ", yarnApplicationState=" + report.getYarnApplicationState().toString()
                    + ", finalApplicationStatus=" + report.getFinalApplicationStatus().toString()
                    + ", trackingUrl=" + report.getTrackingUrl()
                    + ", user=" + report.getUser());


            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus status = report.getFinalApplicationStatus();
            switch (state) {

                case FAILED: {
                    stop();
                    LOG.info("Application did not finish. YarnState={}, DSFinalStatus={}", state, status);
                    break;
                }

                case KILLED: {
                    stop();
                    LOG.info("Application did not finish. YarnState={}, DSFinalStatus={}", state, status);
                    break;
                }


                case FINISHED: {
                    stop();
                    if (status == FinalApplicationStatus.SUCCEEDED) {
                        LOG.info("Application has completed successfully.");
                    } else {
                        LOG.info("Application did finished unsuccessfully. YarnState={}, DSFinalStatus={}.", state, status);
                    }
                }

                default:
            }

        });
    }

    @Nullable
    private ApplicationReport getApplicationReport() {
        try {
            return anYarnClient.getApplicationReport(anApplicationId);
        } catch (YarnException | IOException e) {
            LOG.error("Cannot get report for application with id {}", anApplicationId);
            stop();
            return null;
        }
    }

}

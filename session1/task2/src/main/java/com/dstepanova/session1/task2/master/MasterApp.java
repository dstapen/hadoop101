package com.dstepanova.session1.task2.master;

import com.dstepanova.session1.task2.Context;

import com.dstepanova.session1.task2.EnrichResourcesCommand;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;

import java.io.IOException;

import static com.dstepanova.session1.task2.Requirements.requireNotNullArgument;
import static com.dstepanova.session1.task2.Requirements.requireNotNullState;

import static org.slf4j.LoggerFactory.getLogger;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.dstepanova.session1.task2.SystemVariables;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;


public class MasterApp implements Runnable, AutoCloseable {
    private static final Logger LOG = getLogger(MasterApp.class);

    public enum Event {
        APP_ATTEMPT_START, APP_ATTEMPT_END, CONTAINER_START, CONTAINER_END
    }

    public enum Entity {
        APP_ATTEMPT, CONTAINER
    }

    private final Context aContext;
    private final Configuration aConfiguration;
    private final FileSystem aFileSystem;

    @SuppressWarnings("rawtypes")
    private AMRMClientAsync amRMClient;

    private NMClientAsync nmClientAsync;
    private NMCallbackHandler containerListener;
    protected ApplicationAttemptId appAttemptID;
    private int appMasterRpcPort = -1;
    private String appMasterTrackingUrl = "";
    protected int numTotalContainers = 1;
    private int containerMemory = 10;
    private int containerVirtualCores = 1;
    private int requestPriority;
    private AtomicInteger numCompletedContainers = new AtomicInteger();
    protected AtomicInteger numAllocatedContainers = new AtomicInteger();
    private AtomicInteger numFailedContainers = new AtomicInteger();
    protected AtomicInteger numRequestedContainers = new AtomicInteger();
    private volatile boolean done;
    private List<Thread> launchThreads = new ArrayList<>();
    private TimelineClient timelineClient;
    private String jarPath;
    private long jarPathLen;
    private long jarPathTime;



    private static final String appMasterJarPath = "AppMaster.jar";
    private AtomicInteger numberContainers = new AtomicInteger();
    private AtomicInteger numberCompletedContainers = new AtomicInteger();
    private FileBlocks fileBlocks = new FileBlocks();

    public static Builder builder() {
        return new Builder();
    }


    private MasterApp(Context aContext, Configuration aConfiguration, FileSystem aFileSystem) {
        this.aContext = requireNotNullArgument(aContext, "context must not be null");
        this.aConfiguration = requireNotNullArgument(aConfiguration, "configuration must not be null");
        this.aFileSystem = requireNotNullArgument(aFileSystem, "file system must not be null");
    }

    public boolean init() throws ParseException, IOException {
        Map<String, String> envs = System.getenv();

        ContainerId containerId = ConverterUtils.toContainerId(envs.get(Environment.CONTAINER_ID.name()));
        appAttemptID = containerId.getApplicationAttemptId();


        if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
            throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV + " is not specified");
        }
        if (!envs.containsKey(Environment.NM_HOST.name())) {
            throw new RuntimeException(Environment.NM_HOST.name() + " is not specified");
        }
        if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
            throw new RuntimeException(Environment.NM_HTTP_PORT + " is not specified");
        }
        if (!envs.containsKey(Environment.NM_PORT.name())) {
            throw new RuntimeException(Environment.NM_PORT.name() + " is not specified");
        }

        LOG.info("Application master for app" + ", appId="
                + appAttemptID.getApplicationId().getId() + ", clustertimestamp="
                + appAttemptID.getApplicationId().getClusterTimestamp()
                + ", attemptId=" + appAttemptID.getAttemptId());

        jarPath = envs.get(SystemVariables.ARTIFACT);
        jarPathLen = Long.parseLong(envs.get(SystemVariables.LENGTH));
        jarPathTime = Long.parseLong(envs.get(SystemVariables.MODIFICATION));

        containerMemory = 10; // Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
        containerVirtualCores = 1; // Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
        numTotalContainers = 1; //Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
        if (numTotalContainers == 0) {
            throw new IllegalArgumentException("Cannot run distributed shell with no containers");
        }
        requestPriority = 0; //Integer.parseInt(cliParser.getOptionValue("priority", "0"));

        // Creating the Timeline Client
        timelineClient = TimelineClient.createTimelineClient();
        timelineClient.init(aConfiguration);
        timelineClient.start();

        return true;
    }


    @SuppressWarnings({"unchecked"})
    public void go() throws YarnException, IOException, URISyntaxException {
        LOG.info("Starting ApplicationMaster");
        try {
            publishApplicationAttemptEvent(timelineClient, appAttemptID.toString(), Event.APP_ATTEMPT_START);
        } catch (Exception e) {
            LOG.error("App Attempt start event coud not be pulished for " + appAttemptID.toString(), e);
        }

        AMRMClientAsync.CallbackHandler amRmListener = new RMCallbackHandler();
        amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, amRmListener);
        amRMClient.init(aConfiguration);
        amRMClient.start();

        containerListener = new NMCallbackHandler(this);
        nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(aConfiguration);
        nmClientAsync.start();

        String appMasterHostname = NetUtils.getHostname();
        RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(appMasterHostname, appMasterRpcPort, appMasterTrackingUrl);

        modifyRequiredResources(response);

        askContainers();

        try {
            publishApplicationAttemptEvent(timelineClient, appAttemptID.toString(), Event.APP_ATTEMPT_END);
        } catch (Exception e) {
            e.printStackTrace(System.err);
            LOG.error("App Attempt start event coud not be pulished for " + appAttemptID.toString(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private void askContainers() throws IOException {
        Path path = new Path(aContext.getSourceFile());
        FileStatus fileStatus = aFileSystem.getFileStatus(path);
        BlockLocation[] fileBlockLocations = aFileSystem.getFileBlockLocations(fileStatus, 0L, fileStatus.getLen());
        LOG.info("Found " + fileBlockLocations.length + " blocks of file " + aContext.getSourceFile());
        for (BlockLocation blockLocation : fileBlockLocations) {
            fileBlocks.addBlock(new Fragment(blockLocation));

            //
            LOG.info("Asking container for block (" + blockLocation.getOffset() + "," + blockLocation.getLength() + ") of file " + aContext.getSourceFile());
            LOG.info("Asking container for block ({},{}) of file {}",
                    blockLocation.getOffset(),
                    blockLocation.getLength(),
                    aContext.getSourceFile());

            //

            ContainerRequest containerAsk = setupContainerAskForRM(blockLocation.getHosts());
            amRMClient.addContainerRequest(containerAsk);
            LOG.info("Asked container for block (" + blockLocation.getOffset() + "," + blockLocation.getLength() + ") of file " + aContext.getSourceFile());
            numberContainers.incrementAndGet();
        }
    }

    private void modifyRequiredResources(RegisterApplicationMasterResponse response) {
        int maxMem = response.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);
        if (containerMemory > maxMem) {
            LOG.info("Container memory specified above max threshold of cluster. Using max value." + ", specified=" + containerMemory + ", max=" + maxMem);
            containerMemory = maxMem;
        }

        int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
        LOG.info("Max vcores capabililty of resources in this cluster " + maxVCores);
        if (containerVirtualCores > maxVCores) {
            LOG.info("Container virtual cores specified above max threshold of cluster. Using max value." + ", specified=" + containerVirtualCores + ", max=" + maxVCores);
            containerVirtualCores = maxVCores;
        }
    }

    protected boolean finish() {
        while (!done) {
            try {
                LOG.info("Wait for completion...");
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
            }
        }
        LOG.info("Application completed. Stopping running containers");
        nmClientAsync.stop();

        LOG.info("Application completed. Signalling finish to RM");

        FinalApplicationStatus appStatus = FinalApplicationStatus.SUCCEEDED;
        try {
            amRMClient.unregisterApplicationMaster(appStatus, null, null);
        } catch (YarnException | IOException ex) {
            LOG.error("Failed to unregister application", ex);
        }

        amRMClient.stop();

        return true;
    }

    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
        @SuppressWarnings("unchecked")
        @Override
        public void onContainersCompleted(List<ContainerStatus> completedContainers) {
            LOG.info("Got response from RM for container ask, completedCnt="
                    + completedContainers.size());
            for (ContainerStatus containerStatus : completedContainers) {
                LOG.info(appAttemptID + " got container status for containerID="
                        + containerStatus.getContainerId() + ", state="
                        + containerStatus.getState() + ", exitStatus="
                        + containerStatus.getExitStatus() + ", diagnostics="
                        + containerStatus.getDiagnostics());

                if (containerStatus.getExitStatus() == ContainerExitStatus.SUCCESS) {
                    numberCompletedContainers.incrementAndGet();
                    appendResult(containerStatus);
                } else {
                    LOG.error("Container " + containerStatus.getContainerId() + " failed with status " + containerStatus.getExitStatus());
                }
                try {
                    publishContainerEndEvent(timelineClient, containerStatus);
                } catch (Exception e) {
                    LOG.error("Container start event could not be pulished for "
                            + containerStatus.getContainerId().toString(), e);
                }
            }
            if (numberContainers.get() == numberCompletedContainers.get()) {
                done = true;
            }
        }

        private void appendResult(ContainerStatus containerStatus) {
            Path outPath = new Path(aContext.getDestinationFile());
            try {
                if (!aFileSystem.exists(outPath)) {
                    LOG.info("Creating file " + outPath);
                    aFileSystem.createNewFile(outPath);
                }
            } catch (IOException e) {
                LOG.error("Could not create file " + outPath, e);
            }

            String containerTmpFolder = "/tmp/" + containerStatus.getContainerId();
            Path resultPath = new Path(containerTmpFolder + "/result");
            try (
                    InputStream inputStream = aFileSystem.open(resultPath);
                    OutputStream outputStream = aFileSystem.append(outPath)
            ) {
                LOG.info("Copy data from " + resultPath + " to " + outPath);
                IOUtils.copy(inputStream, outputStream);
                LOG.info("Copied data from " + resultPath + " to " + outPath);
            } catch (IOException e) {
                LOG.error("Could not append result", e);
            }

            try {
                LOG.info("Removing folder " + containerTmpFolder);
                aFileSystem.delete(new Path(containerTmpFolder), true);
            } catch (IOException e) {
                LOG.error("Could not remove folder " + containerTmpFolder, e);
            }
        }

        @Override
        public void onContainersAllocated(List<Container> allocatedContainers) {
            LOG.info("Got response from RM for container allocated. Allocated containers number=" + allocatedContainers.size());

            for (Container allocatedContainer : allocatedContainers) {
                LOG.info("Launching shell command on a new container."
                        + ", containerId=" + allocatedContainer.getId()
                        + ", containerNode=" + allocatedContainer.getNodeId().getHost()
                        + ":" + allocatedContainer.getNodeId().getPort()
                        + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
                        + ", containerResourceMemory="
                        + allocatedContainer.getResource().getMemory()
                        + ", containerResourceVirtualCores="
                        + allocatedContainer.getResource().getVirtualCores());

                allocateContainer(allocatedContainer);
            }
        }

        private void allocateContainer(Container allocatedContainer) {
            LOG.info("Set the environment for the application master");
            Map<String, String> env = new HashMap<>();

            String classPathEnv = getClasspath();

            env.put("CLASSPATH", classPathEnv);

            // Set the local resources
            Map<String, LocalResource> localResources = new HashMap<>();

            new EnrichResourcesCommand(localResources, new Path(jarPath), appMasterJarPath, jarPathLen, jarPathTime).run();

            Fragment availableFragment = fileBlocks.nextAvailableFragment(allocatedContainer.getNodeId().getHost());

            String command = Arrays.asList(
                    Environment.JAVA_HOME.$$() + "/bin/java",
                    "-D" + com.dstepanova.session1.task2.container.Container.MACRO_SOURCE + '=' + aContext.getSourceFile(),
                    "-D" + com.dstepanova.session1.task2.container.Container.MACRO_DESTINATION + '=' + aContext.getDestinationFile(),
                    "-D" + com.dstepanova.session1.task2.container.Container.MACRO_SINCE + '=' + availableFragment.getBlockLocation().getOffset(),
                    "-D" + com.dstepanova.session1.task2.container.Container.MACRO_LENGTH + '=' + availableFragment.getBlockLocation().getLength(),
                    com.dstepanova.session1.task2.container.Container.class.getName(),
                    "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/out",
                    "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/err")
                    .stream()
                    .collect(Collectors.joining(" "));

            LOG.info("Container command = " + command);

            List<String> commands = new ArrayList<>();
            commands.add(command);

            ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
                    localResources, env, commands, null, null, null);
            containerListener.addContainer(allocatedContainer.getId(), allocatedContainer);
            nmClientAsync.startContainerAsync(allocatedContainer, ctx);
        }

        private String getClasspath() {
            StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
                    .append(ApplicationConstants.CLASS_PATH_SEPARATOR)
                    .append("./*");

            for (String c : aConfiguration.getStrings(
                    YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                    YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
                classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
                classPathEnv.append(c.trim());
            }
            return classPathEnv.toString();
        }

        @Override
        public void onShutdownRequest() {
            done = true;
        }

        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes) {
        }

        @Override
        public float getProgress() {
            // set progress to deliver to RM on next heartbeat
            return 0;
        }

        @Override
        public void onError(Throwable e) {
            done = true;
            amRMClient.stop();
        }
    }

    static class NMCallbackHandler
            implements NMClientAsync.CallbackHandler {

        private ConcurrentMap<ContainerId, Container> containers =
                new ConcurrentHashMap<>();
        private final MasterApp applicationMaster;

        public NMCallbackHandler(MasterApp applicationMaster) {
            this.applicationMaster = applicationMaster;
        }

        public void addContainer(ContainerId containerId, Container container) {
            containers.putIfAbsent(containerId, container);
        }

        @Override
        public void onContainerStopped(ContainerId containerId) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Succeeded to stop Container " + containerId);
            }
            containers.remove(containerId);
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId,
                                              ContainerStatus containerStatus) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Container Status: id=" + containerId + ", status=" +
                        containerStatus);
            }
        }

        @Override
        public void onContainerStarted(ContainerId containerId,
                                       Map<String, ByteBuffer> allServiceResponse) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Succeeded to start Container " + containerId);
            }
            Container container = containers.get(containerId);
            if (container != null) {
                applicationMaster.nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
            }
            try {
                MasterApp.publishContainerStartEvent(
                        applicationMaster.timelineClient, container);
            } catch (Exception e) {
                LOG.error("Container start event coud not be pulished for "
                        + container.getId().toString(), e);
            }
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to start Container " + containerId);
            containers.remove(containerId);
            applicationMaster.numCompletedContainers.incrementAndGet();
            applicationMaster.numFailedContainers.incrementAndGet();
        }

        @Override
        public void onGetContainerStatusError(
                ContainerId containerId, Throwable t) {
            LOG.error("Failed to query the status of Container " + containerId);
        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to stop Container " + containerId);
            containers.remove(containerId);
        }
    }

    private ContainerRequest setupContainerAskForRM(String[] nodes) {
        Priority pri = Priority.newInstance(requestPriority);

        Resource capability = Resource.newInstance(containerMemory, containerVirtualCores);

        ContainerRequest request = new ContainerRequest(capability, nodes, null, pri);
        LOG.info("Requested container ask: " + request.toString());
        return request;
    }

    private static void publishContainerStartEvent(TimelineClient timelineClient,
                                                   Container container) throws IOException, YarnException {
        TimelineEntity entity = new TimelineEntity();
        entity.setEntityId(container.getId().toString());
        entity.setEntityType(Entity.CONTAINER.toString());
        entity.addPrimaryFilter("user",
                UserGroupInformation.getCurrentUser().getShortUserName());
        TimelineEvent event = new TimelineEvent();
        event.setTimestamp(System.currentTimeMillis());
        event.setEventType(Event.CONTAINER_START.toString());
        event.addEventInfo("Node", container.getNodeId().toString());
        event.addEventInfo("Resources", container.getResource().toString());
        entity.addEvent(event);

        timelineClient.putEntities(entity);
    }

    private static void publishContainerEndEvent(TimelineClient timelineClient,
                                                 ContainerStatus container) throws IOException, YarnException {
        TimelineEntity entity = new TimelineEntity();
        entity.setEntityId(container.getContainerId().toString());
        entity.setEntityType(Entity.CONTAINER.toString());
        entity.addPrimaryFilter("user", UserGroupInformation.getCurrentUser().getShortUserName());
        TimelineEvent event = new TimelineEvent();
        event.setTimestamp(System.currentTimeMillis());
        event.setEventType(Event.CONTAINER_END.toString());
        event.addEventInfo("State", container.getState().name());
        event.addEventInfo("Exit Status", container.getExitStatus());
        entity.addEvent(event);

        timelineClient.putEntities(entity);
    }

    private static void publishApplicationAttemptEvent(
            TimelineClient timelineClient, String appAttemptId, Event appEvent)
            throws IOException, YarnException {
        TimelineEntity entity = new TimelineEntity();
        entity.setEntityId(appAttemptId);
        entity.setEntityType(Entity.APP_ATTEMPT.toString());
        entity.addPrimaryFilter("user", UserGroupInformation.getCurrentUser().getShortUserName());
        TimelineEvent event = new TimelineEvent();
        event.setEventType(appEvent.toString());
        event.setTimestamp(System.currentTimeMillis());
        entity.addEvent(event);

        timelineClient.putEntities(entity);
    }

    @Override
    public void run() {
        try {
            init();
        } catch (ParseException | IOException e) {
            LOG.error("init ----> ", e);
        } finally {
            try {
                go();
            } catch (YarnException | IOException | URISyntaxException e) {
                LOG.error("go ----> ", e);
            }
        }
    }


    @Override
    public void close() throws Exception {
        finish();
    }

    public static class Builder {
        private final Configuration aConfiguration = new YarnConfiguration();

        private Builder() {
        }

        public MasterApp build() {
            FileSystem aFileSystem = acquireFileSystem();
            return new MasterApp(Context.newInstance(), aConfiguration, aFileSystem);
        }

        private FileSystem acquireFileSystem() {
            requireNotNullState(aConfiguration, "configuration must not be null");
            try {
                return FileSystem.get(aConfiguration);
            } catch (IOException e) {
                throw new RuntimeException("cannot acquire file system", e);
            }
        }
    }
}

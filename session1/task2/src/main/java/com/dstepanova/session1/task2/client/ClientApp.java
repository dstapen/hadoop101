package com.dstepanova.session1.task2.client;

import com.dstepanova.session1.task2.DeployCommand;
import com.dstepanova.session1.task2.StateException;
import com.dstepanova.session1.task2.container.InfoMetrics;
import com.dstepanova.session1.task2.SystemVariables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;

import static com.dstepanova.session1.task2.Requirements.requireNotNullState;
import static java.util.Optional.ofNullable;
import static org.apache.hadoop.yarn.api.ApplicationConstants.Environment.JAVA_HOME;
import static org.slf4j.LoggerFactory.getLogger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

final class ClientApp implements Runnable, AutoCloseable {
    private static final Logger LOG = getLogger(ClientApp.class);
    private static final String QUEUE = ofNullable(System.getProperty("queue")).orElse("default");
    private static final boolean KEEP_COUNTAINERS = Boolean.parseBoolean(ofNullable(System.getProperty("keep_containers")).orElse("false"));
    private static int MEMORY_LIMIT = Integer.parseInt(ofNullable(System.getProperty("mem")).orElse("10"));
    private static int CPU_LIMIT = Integer.parseInt(ofNullable(System.getProperty("cpu")).orElse("10"));
    private static final int PRIORITY = Integer.parseInt(ofNullable(System.getProperty("priority")).orElse("0"));

    private final ClientContext aContext;
    private final Configuration aConfiguration;
    private final YarnClient anYarnClient;
    private final ManagedMonitor aManagedMonitor;


    public ClientApp(ClientContext aContext,
                     Configuration aConfiguration,
                     YarnClient anYarnClient,
                     ManagedMonitor aManagedMonitor) {
        this.aContext = aContext;
        this.aConfiguration = aConfiguration;
        this.anYarnClient = anYarnClient;
        this.aManagedMonitor = aManagedMonitor;
    }



    @Override
    public void run() {
        LOG.info("Application is up and running...");
        aManagedMonitor.start(3, TimeUnit.SECONDS);
        aManagedMonitor.await();
    }

    @Override
    public void close() throws Exception {
        LOG.info("Application is about to stop...");
        System.exit(0);
    }

    @Nonnull
    public static Builder builder() {
        return new Builder();
    }

    private static void printClusterInfo(YarnClient anYarnClient) {
        try {
            YarnClusterMetrics clusterMetrics = anYarnClient.getYarnClusterMetrics();
            LOG.info("Cluster metric info from ASM, \n numNodeManagers = {}",
                    clusterMetrics.getNumNodeManagers());

            List<NodeReport> clusterNodeReports = anYarnClient.getNodeReports(NodeState.RUNNING);
            LOG.info("Cluster node info");
            for (NodeReport node : clusterNodeReports) {
                LOG.info("ASM report: nodeId = {}, nodeAddress = {}, " +
                        "nodeRackName = {}, nodeNumContainers = {}",
                        node.getNodeId(), node.getHttpAddress(),
                        node.getRackName(), node.getNumContainers());
            }

            QueueInfo queueInfo = anYarnClient.getQueueInfo(QUEUE);
            LOG.info("Queue info: \n queueName = {}, queueCurrentCapacity = {}, " +
                    "queueMaxCapacity = {}, queueApplicationCount = {}, queueChildQueueCount = {}",
                  queueInfo.getQueueName(), queueInfo.getCurrentCapacity(),
                           queueInfo.getMaximumCapacity(), queueInfo.getApplications().size(),
                           queueInfo.getChildQueues().size());

            List<QueueUserACLInfo> listAclInfo = anYarnClient.getQueueAclsInfo();
            for (QueueUserACLInfo aclInfo : listAclInfo) {
                for (QueueACL userAcl : aclInfo.getUserAcls()) {
                    LOG.info("queueName = {}, userAcl = {}",
                            aclInfo.getQueueName(), userAcl.name());
                }
            }
        } catch (YarnException | IOException e) {
            throw new StateException("Failed to get metrics", e);
        }
    }

    public static class Builder {
        String aMainMasterClass;
        ClientContext aClientContext;
        YarnConfiguration aConfiguration;
        YarnClient anYarnClient;
        ManagedMonitor aManagedMonitor;

        public Builder forClass(Class<?> aClass) {
            aMainMasterClass = requireNotNullState(aClass, "main master class must not be null").getName();
            return this;
        }

        private void modifyRequiredResources(GetNewApplicationResponse appResponse) {
            int maxMem = appResponse.getMaximumResourceCapability().getMemory();
            LOG.info("Max memory = {}", maxMem);
            if (MEMORY_LIMIT > maxMem) {
                LOG.info("Memory limit = {}", MEMORY_LIMIT);
                MEMORY_LIMIT = maxMem;
            }

            int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
            LOG.info("current cores = {}", maxVCores);
            if (CPU_LIMIT > maxVCores) {
                LOG.info("cpu limit = {}", + CPU_LIMIT);
                CPU_LIMIT = maxVCores;
            }
        }


        @Nonnull
        public ClientApp build() {
            LOG.info("Initializing application...");

            try {

                aClientContext = ClientContext.newInstance(aMainMasterClass);

                aConfiguration = new YarnConfiguration();

                anYarnClient = YarnClient.createYarnClient();
                anYarnClient.init(aConfiguration);
                anYarnClient.start();

                printClusterInfo(anYarnClient);

                YarnClientApplication app = anYarnClient.createApplication();
                GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

                modifyRequiredResources(appResponse);

                ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();

                appContext.setKeepContainersAcrossApplicationAttempts(KEEP_COUNTAINERS);
                appContext.setApplicationName(aClientContext.getApplicationName());

                Map<String, LocalResource> localResources = new HashMap<>();

                LOG.info("Deploy artefact into hfds ...");
                FileSystem aFileSystem = FileSystem.get(aConfiguration);
                InfoMetrics jarFileInfo = new DeployCommand(aFileSystem,
                        aClientContext.getDeploy(),
                        aClientContext.getDeploy(),
                        appContext.getApplicationId().toString(),
                        localResources,
                        aClientContext.getApplicationName()).call();

                Map<String, String> env = getEnvVars(jarFileInfo);
                StringBuilder classPathEnv = getClassPath();

                env.put("CLASSPATH", classPathEnv.toString());


                String command = Arrays.asList(JAVA_HOME.$$() + "/bin/java",
                        "-Xmx" + MEMORY_LIMIT + "m",
                        aClientContext.toCommandLineOptions(),
                        "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/out.log",
                        "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/err.log")
                        .stream().collect(Collectors.joining(" "));

                LOG.info("Next command will be executed: {} \n" + command);
                List<String> commands = new ArrayList<>();
                commands.add(command);

                ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
                        localResources, env, commands, null, null, null);

                Resource capability = Resource.newInstance(MEMORY_LIMIT, CPU_LIMIT);
                appContext.setResource(capability);
                appContext.setAMContainerSpec(amContainer);

                Priority pri = Priority.newInstance(PRIORITY);
                appContext.setPriority(pri);
                appContext.setQueue(QUEUE);

                anYarnClient.submitApplication(appContext);
                aManagedMonitor = new ManagedMonitor(anYarnClient, appContext.getApplicationId());
                return new ClientApp(aClientContext, aConfiguration, anYarnClient, aManagedMonitor);
            } catch (YarnException | IOException e) {
                throw new StateException(e);
            }
        }

        private StringBuilder getClassPath() {
            StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
                    .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
            for (String c : aConfiguration.getStrings(
                    YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                    YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
                classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
                classPathEnv.append(c.trim());
            }
            return classPathEnv;
        }

        private Map<String, String> getEnvVars(InfoMetrics jarFileInfo) {
            Map<String, String> env = new HashMap<>();
            env.put(SystemVariables.ARTIFACT, jarFileInfo.getPath().toString());
            env.put(SystemVariables.LENGTH, jarFileInfo.getLength().toString());
            env.put(SystemVariables.MODIFICATION, jarFileInfo.getModificationTime().toString());
            return env;
        }

    }
}
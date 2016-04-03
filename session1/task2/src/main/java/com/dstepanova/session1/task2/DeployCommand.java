package com.dstepanova.session1.task2;

import com.dstepanova.session1.task2.container.InfoMetrics;
import com.google.common.base.Joiner;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;

public class DeployCommand implements Callable<InfoMetrics> {
    private final FileSystem aFileSystem;
    private final String aSourcePath;
    private final String aDestinationPath;
    private final String anApplicationIdentity;
    private final Map<String, LocalResource> resources;
    private final String anApplicationName;

    public DeployCommand(FileSystem aFileSystem, String aSourcePath, String aDestinationPath, String anApplicationIdentity, Map<String, LocalResource> resources, String anApplicationName) {
        this.aFileSystem = aFileSystem;
        this.aSourcePath = aSourcePath;
        this.aDestinationPath = aDestinationPath;
        this.anApplicationIdentity = anApplicationIdentity;
        this.resources = resources;
        this.anApplicationName = anApplicationName;
    }

    @Override
    public InfoMetrics call() {
        try {
            Path dst = new Path(aFileSystem.getHomeDirectory(), Joiner.on('/')
                    .join(anApplicationName, anApplicationIdentity, aDestinationPath));
            aFileSystem.copyFromLocalFile(new Path(aSourcePath), dst);
            FileStatus fileStatus = aFileSystem.getFileStatus(dst);
            new EnrichResourcesCommand(resources, dst, aDestinationPath, fileStatus.getLen(), fileStatus.getModificationTime()).run();
            return new InfoMetrics(dst, fileStatus.getLen(), fileStatus.getModificationTime());
        } catch (IOException e) {
            throw new StateException("cannot deploy", e);
        }
    }
}

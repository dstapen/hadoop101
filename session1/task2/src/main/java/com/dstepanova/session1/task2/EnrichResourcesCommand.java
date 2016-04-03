package com.dstepanova.session1.task2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;

import java.util.Map;

import static org.apache.hadoop.yarn.api.records.LocalResource.newInstance;
import static org.apache.hadoop.yarn.util.ConverterUtils.getYarnUrlFromURI;

public class EnrichResourcesCommand implements Runnable {
    private final Map<String, LocalResource> localResources;
    private final Path filePath;
    private final String destinationPath;
    private final long length;
    private final long anUpdateTime;

    public EnrichResourcesCommand(Map<String, LocalResource> localResources, Path filePath, String destinationPath, long length, long anUpdateTime) {
        this.localResources = localResources;
        this.filePath = filePath;
        this.destinationPath = destinationPath;
        this.length = length;
        this.anUpdateTime = anUpdateTime;
    }

    @Override
    public void run() {
        localResources.put(destinationPath, newInstance(getYarnUrlFromURI(filePath.toUri()), LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, length, anUpdateTime));
    }
}

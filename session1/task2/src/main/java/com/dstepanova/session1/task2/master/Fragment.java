package com.dstepanova.session1.task2.master;

import org.apache.hadoop.fs.BlockLocation;

public class Fragment {

    private BlockLocation blockLocation;
    private String containerId;
    private IterationStatus status = IterationStatus.AVAILABLE;

    public Fragment(BlockLocation blockLocation) {
        this.blockLocation = blockLocation;
    }

    public BlockLocation getBlockLocation() {
        return blockLocation;
    }

    public void setBlockLocation(BlockLocation blockLocation) {
        this.blockLocation = blockLocation;
    }

    public String getContainerId() {
        return containerId;
    }

    public void setContainerId(String containerId) {
        this.containerId = containerId;
    }

    public IterationStatus getStatus() {
        return status;
    }

    public void setStatus(IterationStatus status) {
        this.status = status;
    }
}

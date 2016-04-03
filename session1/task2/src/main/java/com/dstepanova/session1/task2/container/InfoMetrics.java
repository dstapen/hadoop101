package com.dstepanova.session1.task2.container;

import org.apache.hadoop.fs.Path;

public class InfoMetrics {

    private Path path;
    private Long length;
    private Long modificationTime;

    public InfoMetrics(Path path, Long length, Long modificationTime) {
        this.path = path;
        this.length = length;
        this.modificationTime = modificationTime;
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public Long getLength() {
        return length;
    }

    public void setLength(Long length) {
        this.length = length;
    }

    public Long getModificationTime() {
        return modificationTime;
    }

    public void setModificationTime(Long modificationTime) {
        this.modificationTime = modificationTime;
    }
}

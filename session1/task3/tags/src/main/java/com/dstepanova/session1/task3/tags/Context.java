package com.dstepanova.session1.task3.tags;

import org.slf4j.Logger;

import java.io.File;

import static com.dstepanova.session1.task3.Requirements.requireNotNullAndNotBlankState;
import static org.slf4j.LoggerFactory.getLogger;

public class Context {
    private static final Logger LOG = getLogger(Context.class);
    private final String aSourceFolder;
    private final String aCacheFile;
    private final String aTarget;

    Context() {
        aSourceFolder = System.getProperty("src");
        aCacheFile = System.getProperty("enrich");
        aTarget = System.getProperty("out");
    }

    String[] toArguments() {
        requireNotNullAndNotBlankState(aSourceFolder, "source folder must not be blank");
        requireNotNullAndNotBlankState(aCacheFile, "cache file must not be blank");
        requireNotNullAndNotBlankState(aTarget, "target must not be blank");
        LOG.info("context {}", this.toString());
        return new String[] {aSourceFolder, aTarget, aCacheFile};
    }

    static String grabJavaArchive() {
        return new File(Context.class.getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .getPath())
                .getName();
    }

    @Override
    public String toString() {
        return "Context{" +
                "aSourceFolder='" + aSourceFolder + '\'' +
                ", aTarget='" + aTarget + '\'' +
                '}';
    }
}

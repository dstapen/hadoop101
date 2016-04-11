package com.dstepanova.session1.task4;

import org.slf4j.Logger;

import static com.dstepanova.session1.task4.Requirements.requireNotNullAndNotBlankState;
import static com.dstepanova.session1.task4.Requirements.requireState;
import static org.slf4j.LoggerFactory.getLogger;

public final class Context {
    private final static Logger LOG = getLogger(Context.class);

    private final String aSourceFolder;
    private final String aTarget;
    private final String aReducersNumber;

    Context() {
        aSourceFolder = System.getProperty("src");
        aTarget = System.getProperty("out");
        aReducersNumber = System.getProperty("num");
    }

    String[] toArguments() {
        requireNotNullAndNotBlankState(aSourceFolder, "source folder must not be blank");
        requireNotNullAndNotBlankState(aTarget, "target must not be blank");
        requireNotNullAndNotBlankState(aReducersNumber, "reducers number must not be blank");
        requireState(Integer.parseInt(aReducersNumber) > 0, "reducers number must be positive");
        LOG.info("context {}", this.toString());
        return new String[] {aSourceFolder, aTarget, aReducersNumber};
    }
}

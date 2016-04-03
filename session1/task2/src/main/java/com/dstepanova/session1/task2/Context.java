package com.dstepanova.session1.task2;

import javax.annotation.Nonnull;

import java.util.StringJoiner;

import static com.dstepanova.session1.task2.Requirements.requireNotNullAndNotBlankArgument;
import static com.dstepanova.session1.task2.Requirements.requireNotNullArgument;

public class Context {
    public static final String PARAM_APPLICATION_NAME = "app";
    public static final String PARAM_SOURCE_FILE = "src";
    public static final String PARAM_DESTINATION_FILE = "dest";
    private final String anApplicationName;
    private final String aSourceFile;
    private final String aDestinationFile;

    @Nonnull
    public static Context newInstance() {
        String anApplicationName = requireNotNullAndNotBlankArgument(System.getProperty(PARAM_APPLICATION_NAME), "application name must not be blank.");
        String aSourceFile = requireNotNullAndNotBlankArgument(System.getProperty(PARAM_SOURCE_FILE), "source file must not be blank.");
        String aDestinationFile = requireNotNullAndNotBlankArgument(System.getProperty(PARAM_DESTINATION_FILE), "destination file must not be blank.");
        return new Context(anApplicationName, aSourceFile, aDestinationFile);
    }

    protected Context(Context that) { // copy constructor
        requireNotNullArgument(that, "context is not specified");
        this.anApplicationName = that.anApplicationName;
        this.aSourceFile = that.aSourceFile;
        this.aDestinationFile = that.aDestinationFile;
    }

    protected Context(String anApplicationName, String aSourceFile, String aDestinationFile) {
        this.anApplicationName = anApplicationName;
        this.aSourceFile = aSourceFile;
        this.aDestinationFile = aDestinationFile;
    }

    @Nonnull
    public String toCommandLineOptions() {
        return new StringJoiner(" ")
                .add("-D" + PARAM_SOURCE_FILE + '=' + aSourceFile)
                .add("-D" + PARAM_DESTINATION_FILE + "=" + aDestinationFile)
                .add("-D" + PARAM_APPLICATION_NAME + "=" + anApplicationName)
                .toString();


        //" -D" + PARAM_SOURCE_FILE + '=' + aSourceFile +
        //        " -D" + PARAM_DESTINATION_FILE + '=' + aDestinationFile;

//        new StringJoiner(" ")
//                .add(super.toCommandLineOptions())
//                .add(aMainMasterClass)
//                .add("jar")
//                .add(aJavaArchive)
//                .toString();
    }

    @Nonnull
    public String getApplicationName() {
        return anApplicationName;
    }

    @Nonnull
    public String getSourceFile() {
        return aSourceFile;
    }

    @Nonnull
    public String getDestinationFile() {
        return aDestinationFile;
    }
}

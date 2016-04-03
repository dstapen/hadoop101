package com.dstepanova.session1.task2.client;

import com.dstepanova.session1.task2.Context;
import com.dstepanova.session1.task2.Requirements;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.StringJoiner;

import static com.dstepanova.session1.task2.Requirements.requireNotNullArgument;

public final class ClientContext extends Context {
    private final static String PARAM_DEPLOY = "deploy";
    private final String aDeploy;
    private final String aJavaArchive;
    private final String aMainMasterClass;

    @Nonnull
    public static ClientContext newInstance(String aMainMasterClass) {
        Requirements.requireNotNullAndNotBlankArgument(aMainMasterClass, "main master class must not be blank");
        String aDeploy = requireNotNullArgument(System.getProperty(PARAM_DEPLOY), "deployment is not specified");
        String aJavaArchive = requireNotNullArgument(grabJavaArchive(), "cannot recognize particular java archive");
        return new ClientContext(Context.newInstance(), aMainMasterClass, aJavaArchive, aDeploy);
    }

    private ClientContext(Context context, String aMainMasterClass, String aJavaArchive, String aDeploy) {
        super(context);
        this.aMainMasterClass = aMainMasterClass;
        this.aJavaArchive = aJavaArchive;
        this.aDeploy = aDeploy;
    }

    @Nonnull
    public String getMainMasterClass() {
        return aMainMasterClass;
    }

    @Nonnull
    public String getJavaArchive() {
        return aJavaArchive;
    }

    @Nonnull
    public String getDeploy() {
        return aDeploy;
    }


    @Nonnull
    @Override
    public String toCommandLineOptions() {
        return new StringJoiner(" ")
                .add(super.toCommandLineOptions())
                .add(aMainMasterClass)
                .add("jar")
                .add(aJavaArchive)
                .toString();
    }

    private static String grabJavaArchive() {
        return new File(ClientContext.class.getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .getPath())
                .getName();
    }
}

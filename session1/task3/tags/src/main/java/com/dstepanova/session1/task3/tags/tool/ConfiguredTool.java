package com.dstepanova.session1.task3.tags.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static com.dstepanova.session1.task3.Requirements.requireNotNullAndNotBlankArgument;
import static org.slf4j.LoggerFactory.getLogger;

public class ConfiguredTool extends Configured implements Tool, Runnable, AutoCloseable {
    private static final Logger LOG = getLogger(ConfiguredTool.class);
    private Configuration aConfiguration;
    private Job aJob;


    @Override
    public int run(String[] strings) throws Exception {
        try (ConfiguredTool instance = init(strings)) {
            instance.run();
            return 0;
        } catch (Exception e) {
            LOG.error("FATAL ERROR", e);
            return 1;
        }
    }

    @Override
    public void run() {
        try {
            if (aJob.waitForCompletion(true)) {
                LOG.info("SUCCESS");
                aJob.getCounters()
                        .getGroup("Browser Group")
                        .forEach(counter -> LOG.info("browser " + counter.getName() + " count " + counter.getValue()));

            } else {
                LOG.info("FAILURE");
            }
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            LOG.info("fatal error", e);
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("instance is shutting down.");
    }

    private ConfiguredTool init(String[] strings) throws IOException, URISyntaxException {
        LOG.info("instance is initializing.");
        String aSourceFolder = requireNotNullAndNotBlankArgument(strings[0], "source folder must not be blank.");
        String aTargetFile = requireNotNullAndNotBlankArgument(strings[1], "target file must not be blank.");
        String aCacheFile = requireNotNullAndNotBlankArgument(strings[2], "cache file must not be blank.");

        aConfiguration = getConf();
        aConfiguration.set(TextOutputFormat.SEPERATOR, ",");

        aJob = Job.getInstance(aConfiguration, "tags");
        aJob.setJarByClass(getClass());
        aJob.setMapperClass(ManagedMapper.class);
        aJob.setCombinerClass(ManagedReducer.class);
        aJob.setReducerClass(ManagedReducer.class);
        aJob.addCacheFile(new URI(aCacheFile));
        aJob.setMapOutputKeyClass(Text.class);
        aJob.setMapOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPaths(aJob, aSourceFolder);
        FileOutputFormat.setOutputPath(aJob, new Path(aTargetFile));
        return this;
    }
}

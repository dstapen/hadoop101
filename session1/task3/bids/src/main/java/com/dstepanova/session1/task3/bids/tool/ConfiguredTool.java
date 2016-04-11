package com.dstepanova.session1.task3.bids.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;

import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;

import static com.dstepanova.session1.task3.Requirements.requireNotNullAndNotBlankArgument;
import static org.slf4j.LoggerFactory.getLogger;

public final class ConfiguredTool extends Configured implements Tool, Runnable, AutoCloseable {
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
        LOG.info("instance is up and running.");
        try {
            if (aJob.waitForCompletion(true)) {
                LOG.info("SUCCESS");
                aJob.getCounters()
                        .getGroup("Browser Group")
                        .forEach(counter -> LOG.info("{} : {}", counter.getName(), counter.getValue()));

            } else {
                LOG.info("FAILURE");
            }
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            LOG.error("fatal error", e);
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("Instance is shutting down.");
    }

    public ConfiguredTool init(String[] strings) throws IOException {
        aJob = Job.getInstance(aConfiguration = getConf(), "bids");
        String aSourceFolder = requireNotNullAndNotBlankArgument(strings[0], "source folder must not be blank.");
        String aTargetFile = requireNotNullAndNotBlankArgument(strings[1], "target file must not be blank.");

        aJob.setJarByClass(getClass());
        aJob.setMapperClass(ManagedMapper.class);
        aJob.setCombinerClass(ManagedReducer.class);
        aJob.setReducerClass(ManagedReducer.class);
        aJob.setMapOutputKeyClass(Text.class);
        aJob.setMapOutputValueClass(PayloadValueObject.class);
        aJob.setOutputKeyClass(Text.class);
        aJob.setOutputValueClass(PayloadValueObject.class);

        FileInputFormat.addInputPaths(aJob, aSourceFolder);
        FileOutputFormat.setOutputPath(aJob, new Path(aTargetFile));
        aJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setCompressOutput(aJob, true);
        FileOutputFormat.setOutputCompressorClass(aJob, SnappyCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(aJob, SequenceFile.CompressionType.BLOCK);
        return this;
    }
}

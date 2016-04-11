package com.dstepanova.session1.task4.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;

import java.io.IOException;

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
                        .forEach(counter -> LOG.info("browser {} = count {}", counter.getName(), counter.getValue()));

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

    private ConfiguredTool init(String[] strings) throws IOException {
        LOG.info("instance is initializing.");

        final String aSourceFolder = strings[0];
        final String aTarget = strings[1];
        final int aReducersNumber = Integer.parseInt(strings[2]);

        aConfiguration = getConf();
        aJob = Job.getInstance(aConfiguration, "Task4");
        aJob.setJarByClass(getClass());
        aJob.setNumReduceTasks(aReducersNumber);
        aJob.setMapperClass(ManagedMapper.class);
        aJob.setReducerClass(ManagedReducer.class);
        aJob.setPartitionerClass(ManagedPartitioner.class);
        aJob.setGroupingComparatorClass(CompositeComparator.class);
        aJob.setSortComparatorClass(SortComparator.class);
        aJob.setMapOutputKeyClass(CompositeKey.class);
        aJob.setMapOutputValueClass(Text.class);
        aJob.setOutputKeyClass(Text.class);
        aJob.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPaths(aJob, aSourceFolder);
        FileOutputFormat.setOutputPath(aJob, new Path(aTarget));
        return this;
    }
}

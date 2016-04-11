package com.dstepanova.session1.task3.tags.tool;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ManagedMapper extends Mapper<LongWritable, Text, Text, LongWritable>  {
    private static final Logger LOG = LoggerFactory.getLogger(ManagedMapper.class);

    private Text tag = new Text();
    private LongWritable tagsCount = new LongWritable(1);

    private Map<String, List<String>> tags = new HashMap<>();

    private TagsExtractor tagsExtractor = new TagsExtractor();
    private BiddingParser biddingParser = new BiddingParser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        biddingParser.parse(line);
        if(biddingParser.isFailed()) {
            LOG.warn("Could not extract line {}", line);
        }
        String userId = biddingParser.getUserTags();
        List<String> tags = this.tags.get(userId);
        if(tags == null) {
            LOG.warn("Could not find tags by User Id {}", userId);
            return;
        }
        for(String tagString: tags) {
            tag.set(tagString);
            context.write(tag, tagsCount);
        }
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        for(URI cacheFile: context.getCacheFiles()) {
            Files.lines(new File(cacheFile.getPath()).toPath())
                    .forEach(line -> {
                        tagsExtractor.extract(line);
                        if (tagsExtractor.isSuccess()) {
                            tags.put(tagsExtractor.getId(), tagsExtractor.getTags());
                        }
                    });
        }
    }
}

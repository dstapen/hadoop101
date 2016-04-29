package com.dstepanova.session2.task3;

import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.slf4j.LoggerFactory.getLogger;


public class FlumeInterceptor implements Interceptor {

    private static final Logger LOG = getLogger(FlumeInterceptor.class);
    private static final String HDFS_PREFIX = "hdfs://";
    private static final String SEPARATOR = "\t";
    private static final int ID_POSITION = 0;
    private static final int TAGS_POSITION = 1;
    private static final int LINES_ITEMS_COUNT = 6;
    private static final int USER_TAGS_POSITION = 20;
    private static final int LINE_ITEMS_COUNT = 22;

    private final Map<String, String> tags = new HashMap<>();
    private final String path;

    private boolean success = false;
    private String userTags;


    public FlumeInterceptor(String path) {
        this.path = path;
    }

    @Override
    public void initialize() {
        Parser tagsParser = new Parser();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(openStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                tagsParser.parse(line);
                if (tagsParser.isFailed()) {
                    LOG.warn("Can not parse line: {}", line);
                }
                tags.put(tagsParser.getId(), tagsParser.getTags());
            }
            LOG.info("Success. Total tags count is {}", tags.size());
        } catch (Exception e) {
            throw new RuntimeException("Unexpected failure.", e);
        }
    }

    private InputStream openStream() throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_PREFIX), conf);
        return fileSystem.open(new Path(path));
    }

    @Override
    public Event intercept(Event event) {
        String line = new String(event.getBody());
        parse(line);
        if (isFailed()) {
            LOG.warn("Can not parse line: {}", line);
        }
        String userTagsId = getUserTagsId();
        String tags = this.tags.get(userTagsId);
        if (tags != null) {
            event.getHeaders().put("has_tags", "true");
            String lineWithTags = line + "\t" + tags;
            event.setBody(lineWithTags.getBytes());
        } else {
            event.getHeaders().put("has_tags", "false");
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        List<Event> interceptedEvents = new ArrayList<>();
        for (Event event : list) {
            Event interceptedEvent = intercept(event);
            interceptedEvents.add(interceptedEvent);
        }
        return interceptedEvents;
    }

    @Override
    public void close() {
    }


    public void parse(String line) {
        init();

        String[] lineItems = line.split(SEPARATOR);

        if (lineItems.length != LINE_ITEMS_COUNT) {
            LOG.warn("Wrong format in line {}", line);
            return;
        }

        userTags = lineItems[USER_TAGS_POSITION];


        if (isNullOrEmpty(userTags) || userTags.trim().isEmpty()) {
            LOG.warn("lack of user tags in {}", line);
            return;
        }

        success = true;
    }

    private boolean isFailed() {
        return !success;
    }

    private void init() {
        userTags = null;

        success = false;
    }

    public String getUserTagsId() {
        return userTags;
    }


    private static class Parser {
        private String id;
        private String tags;

        private boolean success;

        public void parse(String line) {
            init();
            String[] lineItems = line.split(SEPARATOR);

            if (lineItems.length != LINES_ITEMS_COUNT) {
                LOG.warn("Wrong numbers of items");
                return;
            }

            id = lineItems[ID_POSITION];
            tags = lineItems[TAGS_POSITION];


            if (isNullOrEmpty(id) || id.trim().isEmpty()) {
                LOG.warn("lack of id in the line {}", line);
                return;
            }

            if (isNullOrEmpty(tags) || tags.trim().isEmpty()) {
                LOG.warn("lack of tags in the line {}", line);
                return;
            }
            success = true;
        }

        public boolean isFailed() {
            return !success;
        }

        private void init() {
            id = null;
            tags = null;
            success = false;
        }

        public String getId() {
            return id;
        }

        public String getTags() {
            return tags;
        }

    }
}

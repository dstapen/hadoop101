package com.dstepanova.session1.task3.tags.tool;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class TagsExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(TagsExtractor.class);
    public static final int COLUMNS = 6;
    public static final int ID_COLUMN = 0;
    public static final int TAGS_COLUMN = 1;

    private String id;
    private List<String> tags;

    private boolean success = false;

    public void extract(String line) {
        init();
        String[] lineItems = line.split("\\t");
        if(lineItems.length != COLUMNS) {
            LOG.warn("Wrong numbers of items in line {} expected {} found {}", line, COLUMNS, lineItems.length);
            return;
        }

        id = lineItems[ID_COLUMN];
        if(StringUtils.isBlank(id)) {
            LOG.warn("id is null in the line {}", line);
            return;
        }

        String tagsString = lineItems[TAGS_COLUMN];
        if(StringUtils.isBlank(tagsString)) {
            LOG.warn("tags is null in the line {}", line);
            return;
        }
        tags = Arrays.asList(tagsString.split(" "));

        success = true;
    }

    public boolean isFailed() {
        return !success;
    }

    public boolean isSuccess() {
        return success;
    }

    private void init() {
        id = null;
        tags = null;

        success = false;
    }

    public String getId() {
        return id;
    }

    public List<String> getTags() {
        return tags;
    }
}

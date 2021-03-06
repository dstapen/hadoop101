package com.dstepanova.session1.task4.tool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Parser {

    private static final Logger LOG = LoggerFactory.getLogger(Parser.class);
    public static final int LINE_ITEMS_COUNT = 22;
    public static final int TIMESTAMP_POSITION = 1;
    public static final int I_PIN_YOU_POSITION = 2;
    public static final int STREAM_ID_POSITION = 21;

    private Long timestamp;
    private String iPinyouId;
    private Integer streamId;

    private boolean notFailed = false;

    public void parse(String line) {
        init();
        String[] lineItems = line.split("\t");
        if(lineItems.length != LINE_ITEMS_COUNT) {
            LOG.warn("Wrong numbers in line {}", line);
            return;
        }

        String timestampItem = null;
        try {
            timestampItem = lineItems[TIMESTAMP_POSITION];
            timestamp = Long.parseLong(timestampItem);
        } catch (NumberFormatException e) {
            LOG.warn("Could not parse timestamp {} in line {}", timestampItem, line);
            return;
        }

        iPinyouId = lineItems[I_PIN_YOU_POSITION];
        if(iPinyouId == null) {
            LOG.warn("iPinyouId is null in the line {}", line);
            return;
        }

        String streamIdItem = null;
        try {
            streamIdItem = lineItems[STREAM_ID_POSITION];
            streamId = Integer.parseInt(streamIdItem);
        } catch (NumberFormatException e) {
            LOG.warn("Could not parse biding {} in line {}", streamIdItem, line);
            return;
        }
        notFailed = true;
    }

    public boolean isFailed() {
        return !notFailed;
    }

    private void init() {
        timestamp = null;
        iPinyouId = null;
        streamId = null;

        notFailed = false;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public String getiPinyouId() {
        return iPinyouId;
    }

    public Integer getStreamId() {
        return streamId;
    }

    public boolean isSuccess() {
        return notFailed;
    }
}

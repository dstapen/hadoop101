package com.dstepanova.session1.task3.bids.tool;

import eu.bitwalker.useragentutils.UserAgent;
import org.slf4j.Logger;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.slf4j.LoggerFactory.getLogger;

public class Helper {
    private static final Logger LOG = getLogger(Helper.class);

    public static final int COLUMNS = 22;
    public static final int USER_AGENT_COLUMN = 2;
    public static final int IP_COLUMN = 4;
    public static final int BIDINGS_COLUMN = 18;

    private String ip;
    private Long bidings;
    private String browser;

    private boolean success = false;

    public void extract(String line) {
        init();
        String[] lineItems = line.split("\t");
        if(lineItems.length != COLUMNS) {
            LOG.error("Ignoring following line {}", line);
            return;
        }

        UserAgent userAgent = new UserAgent(lineItems[USER_AGENT_COLUMN]);
        browser = userAgent.getBrowser().getGroup().name();

        ip = lineItems[IP_COLUMN];
        if(isNullOrEmpty(ip)) {
            LOG.error("IP from line {} is null or empty", line);
            return;
        }
        String bidingsItem = null;
        try {
            bidingsItem = lineItems[BIDINGS_COLUMN];
            bidings = Long.parseLong(bidingsItem);
        } catch (NumberFormatException e) {
            LOG.error("Error getting biding {} from line {}", bidingsItem, line);
            return;
        }
        success = true;
    }

    public boolean isFailed() {
        return !success;
    }

    private void init() {
        ip = null;
        bidings = null;
        browser = null;

        success = false;
    }

    public String getIp() {
        return ip;
    }

    public Long getBidings() {
        return bidings;
    }

    public String getBrowser() {
        return browser;
    }

}

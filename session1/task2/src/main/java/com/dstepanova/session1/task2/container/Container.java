package com.dstepanova.session1.task2.container;

import org.apache.tika.exception.TikaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URISyntaxException;

public class Container {
    public static final String MACRO_SOURCE = "src";
    public static final String MACRO_DESTINATION = "dst";
    public static final String MACRO_SINCE = "since";
    public static final String MACRO_LENGTH = "len";

    private static final Logger LOG = LoggerFactory.getLogger(Container.class);

    public static void main(String[] args) throws TikaException, SAXException, IOException, URISyntaxException {
        String containerId = System.getenv("CONTAINER_ID");


        LOG.info("Initialization container " + containerId);
        System.out.println("Initialization container " + containerId);


        String input = System.getProperty(MACRO_SOURCE);
        String output = System.getProperty(MACRO_DESTINATION);
        Long since = Long.parseLong(System.getProperty(MACRO_SINCE));
        Long length = Long.parseLong(System.getProperty(MACRO_LENGTH));

        LOG.info("Started container " + containerId + " with" +
                " input=" + input +
                " output=" + output +
                " since=" + since +
                " length=" + length);

        LinksProcessor linksProcessor = new Pipeline().create();
        linksProcessor.process(input, output, since, length, containerId);


        LOG.info("Container completed");
    }
}

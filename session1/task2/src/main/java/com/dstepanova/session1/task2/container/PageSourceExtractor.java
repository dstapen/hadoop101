package com.dstepanova.session1.task2.container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

public class PageSourceExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(PageSourceExtractor.class);

    public String extract(String link) {
        try {
            URL url = new URL(link);
            URLConnection conn = url.openConnection();


            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()))) {

                String inputLine;
                StringBuilder sb = new StringBuilder();
                while ((inputLine = br.readLine()) != null) {
                    sb.append(inputLine);
                    sb.append('\n');
                }
                return sb.toString();
            }



        } catch (IOException e) {
            LOG.warn("Cannot load page {}", link, e);
            return "";
        }
    }

    public static void main(String[] args) {
        PageSourceExtractor pse = new PageSourceExtractor();
        System.out.println(pse.extract("http://www.miniinthebox.com/oil-pollution-cleaning-automobile-engine-pipe-with-reinigungspistole-spray-gun-tool_p4815979.html"));
    }
}

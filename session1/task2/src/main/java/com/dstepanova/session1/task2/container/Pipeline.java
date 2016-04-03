package com.dstepanova.session1.task2.container;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.tika.parser.AutoDetectParser;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class Pipeline {

    private FileSystem fileSystem;
    private DataFeeder dataFeeder;
    private AutoDetectParser parser;
    private TopWordsAgregator topWordsAgregator;
    private PageSourceExtractor pageSourceExtractor;

    public Pipeline() throws URISyntaxException, IOException {
        parser = new AutoDetectParser();
        dataFeeder = new DataFeeder();
        topWordsAgregator = new TopWordsAgregator();
        pageSourceExtractor = new PageSourceExtractor();
    }

    public LinksProcessor create() throws URISyntaxException, IOException {
        if(fileSystem == null) {
            Configuration conf = new Configuration ();
            URI hdfsUrl = new URI("hdfs:///");
            fileSystem = FileSystem.get(hdfsUrl, conf);
        }
        dataFeeder.setAutoDetectParser(parser);
        LinksProcessor linksProcessor = new LinksProcessor();
        linksProcessor.setFileSystem(fileSystem);
        linksProcessor.setPageSourceExtractor(pageSourceExtractor);
        linksProcessor.setDataFeeder(dataFeeder);
        linksProcessor.setTopWordsAgregator(topWordsAgregator);
        return linksProcessor;
    }

    public void setFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    public void setDataFeeder(DataFeeder dataFeeder) {
        this.dataFeeder = dataFeeder;
    }

    public void setParser(AutoDetectParser parser) {
        this.parser = parser;
    }

    public void setTopWordsAgregator(TopWordsAgregator topWordsAgregator) {
        this.topWordsAgregator = topWordsAgregator;
    }

    public void setLoader(PageSourceExtractor pageSourceExtractor) {
        this.pageSourceExtractor = pageSourceExtractor;
    }
}

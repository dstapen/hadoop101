package com.dstepanova.session1.task2.container;

import com.dstepanova.session1.task2.StateException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class LinksProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(LinksProcessor.class);

    public static final long TOP_N = 10L;

    public static final String ITEMS_SEPARATOR = "\t";
    public static final int WORDS_POSITION = 1;

    private PageSourceExtractor pageSourceExtractor;
    private DataFeeder dataFeeder;
    private TopWordsAgregator topWordsAgregator;

    private FileSystem fileSystem;

    public void process(String srcFilePath, String destinationFilePath, Long offset, Long length, String containerId) throws IOException { //TODO handle IO
        System.out.println("whoa");
        String containerHdfsPath = "/tmp/" + containerId;
        fileSystem.mkdirs(new Path(containerHdfsPath));
        Path resultHdfsPath = new Path(containerHdfsPath + "/result");
        try (
                FSDataInputStream inputStream = fileSystem.open(new Path(srcFilePath));
                BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
                FSDataOutputStream outputStream = fileSystem.create(resultHdfsPath);
                PrintWriter writer = new PrintWriter(outputStream)
        ) {
            Splitter splitter = new Splitter(bufferedInputStream, offset, length, true);

            Spliterator<String> spliterator = Spliterators.spliteratorUnknownSize(splitter, Spliterator.ORDERED | Spliterator.NONNULL);
            Stream<String> stream = StreamSupport.stream(spliterator, true);
            stream.filter(StringUtils::isNotBlank)
                    .map(this::mapToInputLinkLine)
                    .map(this::processLine)
                    .forEach(outputLinkLine -> write(writer, outputLinkLine));
        }
    }

    private InputLines mapToInputLinkLine(String line) {
        ArrayList<String> lineItems = new ArrayList<>(Arrays.asList(line.split(ITEMS_SEPARATOR)));
        LOG.info("lineItems = " + lineItems);
        return new InputLines(lineItems);
    }

    private void write(PrintWriter writer, OutputLines outputLines) {
        System.out.println("write");
        InputLines inputLines = outputLines.getInputLines();
        ArrayList<String> lineItems = inputLines.getLineItems();
        String concatenatedWords = outputLines
                .getWords()
                .stream()
                .collect(Collectors.joining(" "));
        lineItems.remove(WORDS_POSITION);
        lineItems.add(WORDS_POSITION, concatenatedWords);
        String line = lineItems
                .stream()
                .collect(Collectors.joining("\t"));
        synchronized (writer) {
            LOG.info("writing line: " + line);
            writer.println(line);
            writer.flush();
        }
    }

    private OutputLines processLine(InputLines linkLine) {
        try {
            LOG.info("processing line " + linkLine);
            String htmlBody = pageSourceExtractor.extract(linkLine.getLink());
            List<String> words = dataFeeder.extractWords(htmlBody);
            List<String> topWords = topWordsAgregator.defineMostlyUsedWords(words, TOP_N);
            LOG.info("top words " + topWords);
            return new OutputLines(linkLine, topWords);
        } catch (StateException e) {
            LOG.error("OutputLines error", e);
            throw new RuntimeException(e);
        }
    }


    public void setPageSourceExtractor(PageSourceExtractor pageSourceExtractor) {
        this.pageSourceExtractor = pageSourceExtractor;
    }

    public void setDataFeeder(DataFeeder dataFeeder) {
        this.dataFeeder = dataFeeder;
    }

    public void setTopWordsAgregator(TopWordsAgregator topWordsAgregator) {
        this.topWordsAgregator = topWordsAgregator;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public void setFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }
}

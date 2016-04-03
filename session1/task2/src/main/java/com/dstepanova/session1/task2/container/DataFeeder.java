package com.dstepanova.session1.task2.container;

import com.dstepanova.session1.task2.StateException;
import org.apache.commons.io.IOUtils;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataFeeder {

    private AutoDetectParser autoDetectParser;

    public List<String> extractWords(String htmlBody) throws StateException {
        String parsedText = parse(htmlBody);
        Matcher matcher = Pattern.compile("\\b[^\\d\\W]+\\b").matcher(parsedText);
        List<String> words = new ArrayList<>();
        while(matcher.find()) {
            words.add(matcher.group().toLowerCase());
        }
        return words;
    }

    private String parse(String htmlBody) throws StateException {
        try {
            System.err.println("HTML:" + htmlBody);
            BodyContentHandler bodyContentHandler = new BodyContentHandler();
            Metadata metadata = new Metadata();
            AutoDetectParser parser = new AutoDetectParser();
            parser.parse(IOUtils.toInputStream(htmlBody), bodyContentHandler, metadata, new ParseContext());
            return bodyContentHandler.toString();
        } catch (IOException | SAXException | TikaException e) {
            e.printStackTrace(System.err);
            throw new StateException(e);
        }
    }

    public AutoDetectParser getAutoDetectParser() {
        return autoDetectParser;
    }

    public void setAutoDetectParser(AutoDetectParser autoDetectParser) {
        this.autoDetectParser = autoDetectParser;
    }
}

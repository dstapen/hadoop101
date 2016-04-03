package com.dstepanova.session1.task2.container;

import java.util.List;

public class OutputLines {

    private InputLines inputLines;
    private List<String> words;

    public OutputLines(InputLines inputLines, List<String> words) {
        this.inputLines = inputLines;
        this.words = words;
    }

    public InputLines getInputLines() {
        return inputLines;
    }

    public void setInputLines(InputLines inputLines) {
        this.inputLines = inputLines;
    }

    public List<String> getWords() {
        return words;
    }

    public void setWords(List<String> words) {
        this.words = words;
    }
}

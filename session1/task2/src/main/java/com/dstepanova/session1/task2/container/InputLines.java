package com.dstepanova.session1.task2.container;

import java.util.ArrayList;

public class InputLines {

    private final int NUM_LINK_COLOMN = 5;

    private ArrayList<String> lineItems;

    public InputLines(ArrayList lineItems) {
        this.lineItems = lineItems;
    }

    public ArrayList<String> getLineItems() {
        return new ArrayList<>(lineItems);
    }

    public String getLink() {
        return lineItems.get(NUM_LINK_COLOMN);
    }

}

package com.dstepanova.session1.task4.tool;

import org.apache.hadoop.io.WritableComparator;


public class SortComparator extends WritableComparator {

    public SortComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(Object foo, Object bar) {
        CompositeKey k1 = (CompositeKey) foo;
        CompositeKey k2 = (CompositeKey) bar;
        int i = k1.getiPinyouId().compareTo(k2.getiPinyouId());
        return i != 0 ? i : k1.getTimestamp().compareTo(k2.getTimestamp());
    }
}

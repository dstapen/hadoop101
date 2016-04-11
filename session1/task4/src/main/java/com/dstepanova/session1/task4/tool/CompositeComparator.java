package com.dstepanova.session1.task4.tool;

import org.apache.hadoop.io.WritableComparator;

public class CompositeComparator extends WritableComparator {

    public CompositeComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(Object a, Object b) {
        CompositeKey k1 = (CompositeKey) a;
        CompositeKey k2 = (CompositeKey) b;

        return k1.getiPinyouId().compareTo(k2.getiPinyouId());
    }
}

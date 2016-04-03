package com.dstepanova.session1.task2.master;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class FileBlocks {

    private List<Fragment> fragments;

    public FileBlocks() {
        fragments = new LinkedList<>();
    }

    public void addBlock(Fragment fragment) {
        fragments.add(fragment);
    }

    public synchronized Fragment nextAvailableFragment(String host) {
        Fragment freeFragment = fragments.stream()
                .filter(block -> presentsOnHost(block, host))
                .filter(block -> IterationStatus.AVAILABLE.equals(block.getStatus()))
                .findFirst()
                .get();
        freeFragment.setStatus(IterationStatus.IN_PROGRESS);
        return freeFragment;
    }

    private boolean presentsOnHost(Fragment fragment, String host) {
        try {
            return Arrays.asList(fragment.getBlockLocation().getHosts())
                    .stream()
                    .anyMatch(blockHost -> blockHost.equals(host));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

package com.alibaba.middleware.race.mom.util;

import java.util.HashMap;

/**
 * Created by dawnwords on 15-8-16.
 */
public class QueueSizeCounter {
    private HashMap<Integer, Integer> counter = new HashMap<Integer, Integer>();
    private int times = 0;

    public void put(int i) {
        Integer old = counter.get(i);
        counter.put(i, old != null ? old + 1 : 1);
        if (++times % 100 == 0) {
            System.out.println(counter);
        }
    }
}

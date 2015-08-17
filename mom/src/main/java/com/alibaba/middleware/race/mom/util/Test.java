package com.alibaba.middleware.race.mom.util;

/**
 * Created by scidb on 15-8-17.
 */
public class Test {
    public static void main(String[] args) {
        long start = System.nanoTime();
        long total = 0;
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            total += System.currentTimeMillis();
        }
        System.out.printf("Time:%d ns\n", System.nanoTime() - start);
    }
}

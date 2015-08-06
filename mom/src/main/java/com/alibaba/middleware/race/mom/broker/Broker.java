package com.alibaba.middleware.race.mom.broker;

/**
 * Created by Dawnwords on 2015/8/6.
 */
public class Broker extends Thread {
    @Override
    public void run() {

    }

    public static void main(String[] args) {
        new Broker().start();
    }
}

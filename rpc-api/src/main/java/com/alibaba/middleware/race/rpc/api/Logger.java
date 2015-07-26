package com.alibaba.middleware.race.rpc.api;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Dawnwords on 2015/7/26.
 */
public class Logger {
    private static boolean isDebug = Boolean.valueOf(System.getProperty("Debug", "false"));
    private static final SimpleDateFormat df = new SimpleDateFormat("[HH:mm:ss:SSS]");

    public static void info(String format, Object... args) {
        if (isDebug) {
            System.out.printf(df.format(new Date()) + format + "\n", args);
        }
    }

    public static void error(String format, Object... args) {
        System.err.printf(df.format(new Date()) + format + "\n", new Date(), args);
    }
}

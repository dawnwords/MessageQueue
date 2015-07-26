package com.alibaba.middleware.race.rpc.demo.test;

import java.io.*;

/**
 * Created by huangsheng.hs on 2015/5/19.
 */
public class ConsumerTest {
    protected static OutputStream getFunctionalOutputStream() throws FileNotFoundException {
        return getOutputStream("function.log");
    }

    protected static OutputStream getPerformanceOutputStream() throws FileNotFoundException {
        return getOutputStream("performance.log");
    }

    private static OutputStream getOutputStream(String pathname) throws FileNotFoundException {
        final FileOutputStream fileOutputStream = new FileOutputStream(new File(pathname));
        return new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                fileOutputStream.write(b);
                System.out.write(b);
            }
        };
    }
}

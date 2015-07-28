package com.alibaba.middleware.race.rpc.demo.test;

import java.io.*;

/**
 * Created by huangsheng.hs on 2015/5/19.
 */
public class ConsumerTest {
    protected static OutputStream getFunctionalOutputStream() throws FileNotFoundException {
        File file = new File("function.log");
        final FileOutputStream fileOutputStream = new FileOutputStream(file);
        return new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                System.out.write(b);
                fileOutputStream.write(b);
            }
        };
    }

    protected static OutputStream getPerformanceOutputStream() throws FileNotFoundException {
        File file = new File("performance.log");
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        return fileOutputStream;
    }
}

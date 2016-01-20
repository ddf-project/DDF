package io.ddf2;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by sangdn on 1/20/16.
 */
public class DDFContextTest {

    @Test
    public void multipleThreadText(){
        int n = 100;
        Thread[] threads = new Thread[n];
        for (int i = 0; i < n; i++) {
            final  String tmp = "" +i;
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    DDFContext.setProperty("data", String.valueOf(tmp));
                    DDFContext.setProperty("key", tmp);
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    assert tmp.equals(DDFContext.getProperty("data"));
                    assert tmp.equals(DDFContext.getProperty("key"));
                }
            });
        }

        for (int i = 0; i < n; i++) {
            threads[i].start();
        }
        for (int i = 0; i < n; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
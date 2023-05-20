package org.huwtl.pgrepl.consumer;

public interface DelayService {
    void delayThreadForMillis(long millis) throws InterruptedException;
}

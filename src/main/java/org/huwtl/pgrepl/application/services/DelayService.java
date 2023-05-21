package org.huwtl.pgrepl.application.services;

public interface DelayService {
    void delayThreadForMillis(long millis) throws InterruptedException;
}

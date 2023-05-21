package org.huwtl.pgrepl.application.services;

public class ThreadSleepingService implements DelayService {
    @Override
    public void delayThreadForMillis(long millis) throws InterruptedException {
        Thread.sleep(millis);
    }
}

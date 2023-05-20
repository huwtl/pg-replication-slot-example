package org.huwtl.pgrepl.consumer

class FakeDelayService implements DelayService {
    private List<Long> delayTimesApplied = []
    private boolean willBeInterrupted = false
    private int numberOfTimesInterrupted = 0

    @Override
    void delayThreadForMillis(long millis) throws InterruptedException {
        if (willBeInterrupted) {
            numberOfTimesInterrupted++
            throw new InterruptedException("delay interrupted for test")
        }
        delayTimesApplied.add(millis)
    }

    boolean noDelaysApplied() {
        delayTimesApplied.empty
    }

    List<Long> delaysApplied() {
        delayTimesApplied
    }

    void willBeInterrupted() {
        willBeInterrupted = true
    }

    int numberOfTimesInterrupted() {
        numberOfTimesInterrupted
    }
}

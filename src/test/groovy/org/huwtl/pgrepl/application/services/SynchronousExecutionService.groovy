package org.huwtl.pgrepl.application.services

import java.util.concurrent.AbstractExecutorService
import java.util.concurrent.TimeUnit

class SynchronousExecutionService extends AbstractExecutorService {
    private boolean shuttingDown = false
    private boolean shutdown = false
    private boolean willShutdownGracefully = true
    private boolean shutdownGracefully = false
    private boolean willBeInterruptedWhenAwaitingTermination = false

    @Override
    void shutdown() {
        shuttingDown = true
    }

    @Override
    List<Runnable> shutdownNow() {
        shuttingDown = false
        shutdown = true
        shutdownGracefully = false
        []
    }

    @Override
    boolean isShutdown() {
        shutdown
    }

    @Override
    boolean isTerminated() {
        shutdown
    }

    @Override
    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        if (shutdown) {
            return true
        }
        if (willBeInterruptedWhenAwaitingTermination) {
            throw new InterruptedException("execution service interrupted for test")
        }
        if (shuttingDown) {
            if (willShutdownGracefully) {
                shuttingDown = false
                shutdown = true
                shutdownGracefully = true
                return true
            }
        }
        return false
    }

    @Override
    void execute(Runnable command) {
        command.run()
    }

    void willFailToShutdownGracefully() {
        willShutdownGracefully = false
    }

    void willShutdownGracefully() {
        willShutdownGracefully = true
    }

    void willBeInterruptedWhenAwaitingTermination() {
        willBeInterruptedWhenAwaitingTermination = true
    }

    boolean failedToShutdownGracefully() {
        !shutdownGracefully
    }

    boolean shutdownGracefully() {
        shutdownGracefully
    }
}

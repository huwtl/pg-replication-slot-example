package org.huwtl.pgrepl.publisher;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

import static java.util.Objects.requireNonNull;
import static org.apache.logging.log4j.LogManager.getLogger;

public final class LoggingPublisher<T> implements Publisher<T> {
    private static final Logger LOGGER = getLogger();

    private final Level logLevel;

    public LoggingPublisher(Level logLevel) {
        this.logLevel = requireNonNull(logLevel);
    }

    public LoggingPublisher() {
        this(Level.INFO);
    }

    @Override
    public void publish(T data) {
        LOGGER.log(logLevel, "publishing {}", data);
    }
}

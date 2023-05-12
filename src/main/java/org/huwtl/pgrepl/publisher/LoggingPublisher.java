package org.huwtl.pgrepl.publisher;

import org.apache.logging.log4j.Logger;

import static org.apache.logging.log4j.LogManager.getLogger;

public class LoggingPublisher implements Publisher {
    private static final Logger LOGGER = getLogger();

    @Override
    public void publish(Data data) {
        LOGGER.info("Data published {}", data);
    }
}

package org.huwtl.pgrepl.application.services.publisher;

import org.apache.logging.log4j.Logger;

import static org.apache.logging.log4j.LogManager.getLogger;

public class CountingPublisher implements Publisher {
    private static final Logger LOGGER = getLogger();

    private long numberOfPublishes = 0;

    @Override
    public void publish(Data data) {
        numberOfPublishes++;
        LOGGER.info("Number of publishes {} - data published {}", numberOfPublishes, data);
    }

    long numberOfPublishes() {
        return numberOfPublishes;
    }
}

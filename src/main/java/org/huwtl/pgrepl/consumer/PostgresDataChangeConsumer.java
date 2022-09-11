package org.huwtl.pgrepl.consumer;

import org.huwtl.pgrepl.publisher.Publisher;

import static java.util.Objects.requireNonNull;

public class PostgresDataChangeConsumer {
    private final Publisher<String> publisher;

    public PostgresDataChangeConsumer(Publisher<String> publisher) {
        this.publisher = requireNonNull(publisher);
    }

    public void consume() {
        publisher.publish("some data");
    }
}

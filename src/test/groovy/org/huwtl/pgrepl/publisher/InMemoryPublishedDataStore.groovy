package org.huwtl.pgrepl.publisher

class InMemoryPublishedDataStore<T> implements Publisher<T>, AutoCloseable {
    private final List<T> published = []

    @Override
    void publish(T data) {
        published.add(data)
    }

    @Override
    void close() {
        published.clear()
    }

    List<T> published() {
        published.asImmutable()
    }
}

package org.huwtl.pgrepl.publisher

class InMemoryPublishedDataStore implements Publisher {
    private final List<Data> published = []

    @Override
    void publish(Data data) {
        published.add(data)
    }

    void reset() {
        published.clear()
    }

    List<Data> published() {
        published.asImmutable()
    }

    boolean empty() {
        return published.isEmpty()
    }
}

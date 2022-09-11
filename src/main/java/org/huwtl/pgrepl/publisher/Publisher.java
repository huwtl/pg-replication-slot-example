package org.huwtl.pgrepl.publisher;

public interface Publisher<T> {
    void publish(T data);
}

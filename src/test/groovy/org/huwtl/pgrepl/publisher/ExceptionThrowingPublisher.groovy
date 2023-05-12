package org.huwtl.pgrepl.publisher

class ExceptionThrowingPublisher implements Publisher {
    private final Publisher delegate
    private Exception exceptionToThrow

    ExceptionThrowingPublisher(Publisher delegate) {
        this.delegate = delegate
    }

    static ExceptionThrowingPublisher willNotThrowException(Publisher delegate) {
        new ExceptionThrowingPublisher(delegate)
                .willNotThrowException()
    }

    @Override
    void publish(Data data) {
        if (exceptionToThrow) {
            throw exceptionToThrow
        }
        delegate.publish(data)
    }

    ExceptionThrowingPublisher willThrowException() {
        exceptionToThrow = new IllegalStateException("fake exception caused by test")
        this
    }

    ExceptionThrowingPublisher willNotThrowException() {
        exceptionToThrow = null
        this
    }

    void reset() {
        willNotThrowException()
    }
}

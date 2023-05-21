package org.huwtl.pgrepl.application.services.publisher

class ExceptionThrowingPublisher implements Publisher {
    private final Publisher delegate
    private Exception exceptionToThrow
    private boolean hasThrownException = false

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
            hasThrownException = true
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

    boolean hasThrownException() {
        return hasThrownException
    }

    void reset() {
        willNotThrowException()
        hasThrownException = false
    }
}

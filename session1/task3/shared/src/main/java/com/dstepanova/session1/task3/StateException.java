package com.dstepanova.session1.task3;

public class StateException extends IllegalStateException {

    public StateException() {
        super();
    }

    public StateException(String s) {
        super(s);
    }

    public StateException(String message, Throwable cause) {
        super(message, cause);
    }

    public StateException(Throwable cause) { // try-with-resources semantics
        super(cause);
    }
}

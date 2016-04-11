package com.dstepanova.session1.task4;

public class ArgumentException extends IllegalArgumentException {

    public ArgumentException() {
        super();
    }

    public ArgumentException(String s) {
        super(s);
    }

    public ArgumentException(String message, Throwable cause) {
        super(message, cause);
    }

    public ArgumentException(Throwable cause) { // try-with-resources semantics
        super(cause);
    }
}

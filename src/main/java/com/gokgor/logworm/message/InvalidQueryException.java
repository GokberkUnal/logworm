package com.gokgor.logworm.message;

/** A request parameter combination that cannot be served; mapped to HTTP 400. */
public class InvalidQueryException extends RuntimeException {

    public InvalidQueryException(String message) {
        super(message);
    }
}

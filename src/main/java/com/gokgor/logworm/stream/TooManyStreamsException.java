package com.gokgor.logworm.stream;

/** The configured number of concurrent live tails is already open; mapped to HTTP 503. */
public class TooManyStreamsException extends RuntimeException {

    public TooManyStreamsException(int max) {
        super("Too many open streams (max " + max + "), try again later");
    }
}

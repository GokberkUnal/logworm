package com.gokgor.logworm.kafka;

/** Wraps checked failures from AdminClient calls (timeout, interruption, execution errors). */
public class KafkaRequestException extends RuntimeException {

    public KafkaRequestException(String message, Throwable cause) {
        super(message, cause);
    }
}

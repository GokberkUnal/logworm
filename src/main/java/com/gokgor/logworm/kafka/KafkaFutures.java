package com.gokgor.logworm.kafka;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.KafkaFuture;

/** Small helper to turn a KafkaFuture into a value with a bounded wait. */
public final class KafkaFutures {

    private KafkaFutures() {
    }

    public static <T> T await(KafkaFuture<T> future, Duration timeout) {
        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaRequestException("Interrupted while waiting for Kafka", e);
        } catch (TimeoutException e) {
            throw new KafkaRequestException("Kafka did not respond within " + timeout, e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new KafkaRequestException("Kafka request failed: " + cause.getMessage(), cause);
        }
    }
}

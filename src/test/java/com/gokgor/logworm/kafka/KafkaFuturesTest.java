package com.gokgor.logworm.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Duration;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;

class KafkaFuturesTest {

    private static final Duration TIMEOUT = Duration.ofMillis(200);

    @Test
    void returnsCompletedValue() {
        assertThat(KafkaFutures.await(KafkaFuture.completedFuture("ok"), TIMEOUT)).isEqualTo("ok");
    }

    @Test
    void rethrowsRuntimeCauseUnwrapped() {
        var future = new KafkaFutureImpl<String>();
        future.completeExceptionally(new UnknownTopicOrPartitionException("nope"));

        assertThatThrownBy(() -> KafkaFutures.await(future, TIMEOUT))
                .isInstanceOf(UnknownTopicOrPartitionException.class);
    }

    @Test
    void wrapsCheckedCause() {
        var future = new KafkaFutureImpl<String>();
        future.completeExceptionally(new IOException("disk"));

        assertThatThrownBy(() -> KafkaFutures.await(future, TIMEOUT))
                .isInstanceOf(KafkaRequestException.class)
                .hasCauseInstanceOf(IOException.class);
    }

    @Test
    void wrapsTimeout() {
        var never = new KafkaFutureImpl<String>();

        assertThatThrownBy(() -> KafkaFutures.await(never, TIMEOUT))
                .isInstanceOf(KafkaRequestException.class)
                .hasMessageContaining("did not respond");
    }
}

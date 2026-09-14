package com.gokgor.logworm.stream;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.gokgor.logworm.kafka.BrowserConsumerFactory;
import com.gokgor.logworm.kafka.LogwormKafkaProperties;
import com.gokgor.logworm.kafka.PartitionResolver;
import com.gokgor.logworm.message.MessageDecoder;

import jakarta.annotation.PreDestroy;

/** Opens one live tail per request on its own virtual thread, bounded by {@code logworm.stream.max-concurrent}. */
@Service
public class LiveTailService {

    private final BrowserConsumerFactory consumerFactory;
    private final MessageDecoder decoder;
    private final StreamProperties properties;
    private final Duration timeout;
    private final Semaphore slots;
    private final ExecutorService executor =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("live-tail-", 0).factory());
    private final AtomicLong streamCounter = new AtomicLong();

    public LiveTailService(BrowserConsumerFactory consumerFactory, MessageDecoder decoder,
            StreamProperties properties, LogwormKafkaProperties kafkaProperties) {
        this.consumerFactory = consumerFactory;
        this.decoder = decoder;
        this.properties = properties;
        this.timeout = kafkaProperties.requestTimeout();
        this.slots = new Semaphore(properties.maxConcurrent());
    }

    public SseEmitter tail(String topic, StreamQuery query) {
        if (!slots.tryAcquire()) {
            throw new TooManyStreamsException(properties.maxConcurrent());
        }
        Consumer<byte[], byte[]> consumer = null;
        try {
            consumer = consumerFactory.create("logworm-tail-" + streamCounter.incrementAndGet());
            // Resolve before returning so unknown topic / partition become normal 404 / 400 responses.
            List<TopicPartition> partitions = PartitionResolver.resolve(consumer, topic, query.partition(), timeout);

            SseEmitter emitter = new SseEmitter(0L); // no timeout: the client decides when to stop
            emitter.onCompletion(slots::release);
            var sink = new SseStreamSink(emitter);
            var tail = new LiveTail(consumer, sink, topic, partitions, query, decoder,
                    properties.heartbeat(), properties.lagSkipFactor(), System::nanoTime);
            executor.execute(tail);
            return emitter;
        } catch (RuntimeException e) {
            if (consumer != null) {
                consumer.close();
            }
            slots.release();
            throw e;
        }
    }

    public int openStreams() {
        return properties.maxConcurrent() - slots.availablePermits();
    }

    @PreDestroy
    void shutdown() {
        executor.shutdownNow();
    }
}

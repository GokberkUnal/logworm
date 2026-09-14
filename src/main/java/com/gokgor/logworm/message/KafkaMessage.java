package com.gokgor.logworm.message;

import java.time.Instant;
import java.util.List;

/**
 * One record as returned by the API.
 *
 * @param value       parsed JSON tree when {@code valueFormat} is "json", otherwise a string; null for tombstones
 * @param valueFormat "json" or "string"
 * @param keySize     key length in bytes (0 when null)
 * @param valueSize   value length in bytes (0 when null)
 */
public record KafkaMessage(
        int partition,
        long offset,
        Instant timestamp,
        String timestampType,
        String key,
        Object value,
        String valueFormat,
        List<MessageHeader> headers,
        int keySize,
        int valueSize) {
}

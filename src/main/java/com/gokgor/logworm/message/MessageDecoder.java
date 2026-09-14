package com.gokgor.logworm.message;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/** Turns raw byte records into API messages. */
@Component
public class MessageDecoder {

    private final ObjectMapper objectMapper;

    public MessageDecoder(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public RawMessage toRaw(ConsumerRecord<byte[], byte[]> record) {
        List<MessageHeader> headers = new ArrayList<>();
        record.headers().forEach(h -> headers.add(new MessageHeader(h.key(), utf8(h.value()))));
        return new RawMessage(record, utf8(record.key()), utf8(record.value()), List.copyOf(headers));
    }

    public KafkaMessage toMessage(RawMessage raw, ValueFormat format) {
        var record = raw.record();
        Object value = raw.value();
        String valueFormat = value == null ? null : "string";

        if (format == ValueFormat.AUTO && looksLikeJson(record.value())) {
            try {
                value = objectMapper.readTree(record.value());
                valueFormat = "json";
            } catch (JacksonException e) {
                // not valid JSON after all; keep the string
            }
        }

        return new KafkaMessage(
                record.partition(),
                record.offset(),
                Instant.ofEpochMilli(record.timestamp()),
                record.timestampType().name(),
                raw.key(),
                value,
                valueFormat,
                raw.headers(),
                record.serializedKeySize() < 0 ? 0 : record.serializedKeySize(),
                record.serializedValueSize() < 0 ? 0 : record.serializedValueSize());
    }

    static String utf8(byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    /** Cheap pre-check so we only pay for JSON parsing on payloads that can possibly be JSON. */
    static boolean looksLikeJson(byte[] bytes) {
        if (bytes == null) {
            return false;
        }
        for (byte b : bytes) {
            if (b == ' ' || b == '\n' || b == '\r' || b == '\t') {
                continue;
            }
            return b == '{' || b == '[';
        }
        return false;
    }
}

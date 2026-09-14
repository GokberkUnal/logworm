package com.gokgor.logworm.message;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/** A record with key/value/headers decoded to UTF-8 strings, before JSON parsing. Used for filtering. */
record RawMessage(ConsumerRecord<byte[], byte[]> record, String key, String value, List<MessageHeader> headers) {
}

package com.gokgor.logworm.message;

/** A single Kafka record header. Headers may repeat, so this is a list element, not a map entry. */
public record MessageHeader(String key, String value) {
}

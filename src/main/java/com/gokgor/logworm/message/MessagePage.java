package com.gokgor.logworm.message;

import java.util.List;

/**
 * @param scanned  number of records read from Kafka before filtering
 * @param messages records that matched, at most {@code limit}
 */
public record MessagePage(String topic, int count, int scanned, List<KafkaMessage> messages) {
}

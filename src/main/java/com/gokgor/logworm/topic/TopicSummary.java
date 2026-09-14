package com.gokgor.logworm.topic;

/** One row in the topic list. */
public record TopicSummary(String name, int partitionCount, int replicationFactor, boolean internal) {
}

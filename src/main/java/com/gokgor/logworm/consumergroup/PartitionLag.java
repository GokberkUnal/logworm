package com.gokgor.logworm.consumergroup;

/**
 * Lag of one partition for one group.
 *
 * @param committedOffset last committed offset, or null if the group never committed for this partition
 * @param endOffset       current log end offset
 * @param lag             endOffset − committedOffset, or null when there is no committed offset
 * @param memberId        member currently assigned to the partition, or null
 */
public record PartitionLag(
        String topic,
        int partition,
        Long committedOffset,
        long endOffset,
        Long lag,
        String memberId) {
}

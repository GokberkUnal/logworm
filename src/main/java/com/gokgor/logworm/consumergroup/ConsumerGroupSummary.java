package com.gokgor.logworm.consumergroup;

import java.util.List;

/**
 * One row in the consumer group list.
 *
 * @param state    e.g. STABLE, EMPTY, PREPARING_REBALANCE, DEAD
 * @param type     CLASSIC or CONSUMER (KIP-848 protocol)
 * @param topics   topics the group has committed offsets for or is assigned to
 * @param totalLag sum of lag over all partitions with a committed offset
 */
public record ConsumerGroupSummary(
        String groupId,
        String state,
        String type,
        boolean simple,
        int memberCount,
        List<String> topics,
        long totalLag) {
}

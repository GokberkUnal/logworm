package com.gokgor.logworm.topic;

import java.util.List;

/**
 * @param messageCount sum of per-partition counts; approximate (see {@link PartitionInfo#messageCount()})
 */
public record TopicDetail(
        String name,
        boolean internal,
        int partitionCount,
        int replicationFactor,
        long messageCount,
        List<PartitionInfo> partitions) {
}

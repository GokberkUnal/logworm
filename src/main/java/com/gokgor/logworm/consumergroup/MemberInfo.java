package com.gokgor.logworm.consumergroup;

import java.util.List;

/**
 * @param groupInstanceId static membership id, or null
 * @param assignments     "topic-partition" strings, sorted
 */
public record MemberInfo(
        String memberId,
        String groupInstanceId,
        String clientId,
        String host,
        List<PartitionRef> assignments) {

    public record PartitionRef(String topic, int partition) {
    }
}

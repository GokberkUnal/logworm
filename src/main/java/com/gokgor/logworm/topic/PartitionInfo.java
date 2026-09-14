package com.gokgor.logworm.topic;

import java.util.List;

/**
 * @param id             partition number
 * @param leader         broker id of the leader, or null if there is none
 * @param replicas       broker ids holding a replica
 * @param inSyncReplicas broker ids currently in sync
 * @param earliestOffset first available offset (log start)
 * @param latestOffset   next offset to be written (log end)
 * @param messageCount   latestOffset - earliestOffset; approximate because of compaction/deletion
 */
public record PartitionInfo(
        int id,
        Integer leader,
        List<Integer> replicas,
        List<Integer> inSyncReplicas,
        long earliestOffset,
        long latestOffset,
        long messageCount) {
}

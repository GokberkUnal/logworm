package com.gokgor.logworm.cluster;

import java.util.List;

/**
 * @param clusterId       cluster id reported by the broker
 * @param controllerId    id of the active controller, or null if unknown
 * @param metadataVersion finalized "metadata.version" feature level (KRaft), or null if unavailable
 * @param brokers         all live brokers
 */
public record ClusterInfo(
        String clusterId,
        Integer controllerId,
        String metadataVersion,
        List<BrokerInfo> brokers) {
}

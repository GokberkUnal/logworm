package com.gokgor.logworm.consumergroup;

import java.util.List;

import com.gokgor.logworm.cluster.BrokerInfo;

public record ConsumerGroupDetail(
        String groupId,
        String state,
        String type,
        boolean simple,
        String partitionAssignor,
        BrokerInfo coordinator,
        List<MemberInfo> members,
        List<PartitionLag> partitions,
        long totalLag) {
}

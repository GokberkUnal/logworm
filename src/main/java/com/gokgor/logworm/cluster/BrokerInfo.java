package com.gokgor.logworm.cluster;

import org.apache.kafka.common.Node;

public record BrokerInfo(int id, String host, int port, String rack) {

    public static BrokerInfo from(Node node) {
        return new BrokerInfo(node.id(), node.host(), node.port(), node.rack());
    }
}

package com.gokgor.logworm.shell;

import org.springframework.shell.core.command.annotation.Command;

import com.gokgor.logworm.cluster.BrokerInfo;
import com.gokgor.logworm.cluster.ClusterService;

import lombok.RequiredArgsConstructor;

@InteractiveShellComponent
@RequiredArgsConstructor
public class ClusterCommands {

    private final ClusterService clusterService;
    private final Tables tables;

    @Command(name = "cluster", group = "Cluster", description = "Cluster id, controller and brokers")
    public String cluster() {
        var info = clusterService.describeCluster();
        var header = "cluster " + info.clusterId()
                + "  controller=" + info.controllerId()
                + "  metadata.version=" + info.metadataVersion() + "\n";
        return header + tables.render(info.brokers(),
                new String[] {"id", "host", "port", "rack"},
                BrokerInfo::id, BrokerInfo::host, BrokerInfo::port, BrokerInfo::rack);
    }
}

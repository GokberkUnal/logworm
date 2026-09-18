package com.gokgor.logworm.shell;

import org.springframework.shell.core.command.annotation.Command;
import org.springframework.shell.core.command.annotation.Option;

import com.gokgor.logworm.kafka.KafkaConnection;

import lombok.RequiredArgsConstructor;

@InteractiveShellComponent
@RequiredArgsConstructor
public class ConnectionCommands {

    private final KafkaConnection connection;
    private final RecentConnections recent;
    private final ShellSession session;

    @Command(name = "connect", group = "Cluster", description = "Switch to another Kafka cluster")
    public String connect(@Option(longName = "servers", required = true, description = "host:port[,host:port]") String servers) {
        var info = connection.connect(servers);
        recent.remember(servers);
        session.put(KafkaConnectionStep.SESSION_KEY, servers);
        return "Connected to cluster " + info.clusterId() + " via " + servers + " (" + info.brokerCount() + " brokers)";
    }

    @Command(name = "connection", group = "Cluster", description = "Show the current Kafka connection")
    public String connection() {
        var info = connection.info();
        return "bootstrap=" + info.bootstrapServers()
                + "  cluster=" + (info.clusterId() != null ? info.clusterId() : "(not verified)")
                + "  brokers=" + info.brokerCount();
    }
}

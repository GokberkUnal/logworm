package com.gokgor.logworm.shell;

import org.springframework.shell.core.command.annotation.Command;
import org.springframework.shell.core.command.annotation.Option;

import com.gokgor.logworm.topic.PartitionInfo;
import com.gokgor.logworm.topic.TopicService;
import com.gokgor.logworm.topic.TopicSummary;

import lombok.RequiredArgsConstructor;

@InteractiveShellComponent
@RequiredArgsConstructor
public class TopicCommands {

    private final TopicService topicService;
    private final Tables tables;

    @Command(name = "topics", group = "Topics", description = "List topics")
    public String topics(
            @Option(longName = "internal", description = "Include internal topics such as __consumer_offsets") boolean internal) {
        var rows = topicService.listTopics().stream()
                .filter(t -> internal || !t.internal())
                .toList();
        return tables.render(rows,
                new String[] {"topic", "partitions", "replication", "internal"},
                TopicSummary::name, TopicSummary::partitionCount, TopicSummary::replicationFactor, TopicSummary::internal);
    }

    @Command(name = "topic", group = "Topics", description = "Partition details of one topic")
    public String topic(@Option(longName = "name", required = true, description = "Topic name") String name) {
        var detail = topicService.getTopic(name);
        var header = detail.name()
                + "  partitions=" + detail.partitionCount()
                + "  replication=" + detail.replicationFactor()
                + "  messages≈" + detail.messageCount() + "\n";
        return header + tables.render(detail.partitions(),
                new String[] {"partition", "leader", "replicas", "isr", "earliest", "latest", "messages"},
                PartitionInfo::id, PartitionInfo::leader, PartitionInfo::replicas, PartitionInfo::inSyncReplicas,
                PartitionInfo::earliestOffset, PartitionInfo::latestOffset, PartitionInfo::messageCount);
    }
}

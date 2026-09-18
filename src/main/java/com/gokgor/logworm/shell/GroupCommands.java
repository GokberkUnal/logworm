package com.gokgor.logworm.shell;

import org.springframework.shell.core.command.annotation.Command;
import org.springframework.shell.core.command.annotation.Option;

import com.gokgor.logworm.consumergroup.ConsumerGroupService;
import com.gokgor.logworm.consumergroup.ConsumerGroupSummary;
import com.gokgor.logworm.consumergroup.PartitionLag;

import lombok.RequiredArgsConstructor;

@InteractiveShellComponent
@RequiredArgsConstructor
public class GroupCommands {

    private final ConsumerGroupService consumerGroupService;
    private final Tables tables;

    @Command(name = "groups", group = "Consumer groups", description = "List consumer groups with total lag")
    public String groups() {
        return tables.render(consumerGroupService.listGroups(),
                new String[] {"group", "state", "type", "members", "topics", "lag"},
                ConsumerGroupSummary::groupId, ConsumerGroupSummary::state, ConsumerGroupSummary::type,
                ConsumerGroupSummary::memberCount, g -> String.join(", ", g.topics()), ConsumerGroupSummary::totalLag);
    }

    @Command(name = "group", group = "Consumer groups", description = "Members and per-partition lag of one group")
    public String group(@Option(longName = "id", required = true, description = "Group id") String id) {
        var detail = consumerGroupService.getGroup(id);
        var header = detail.groupId()
                + "  state=" + detail.state()
                + "  members=" + detail.members().size()
                + "  lag=" + detail.totalLag()
                + (detail.coordinator() != null ? "  coordinator=" + detail.coordinator().id() : "") + "\n";
        return header + tables.render(detail.partitions(),
                new String[] {"topic", "partition", "committed", "end", "lag", "member"},
                PartitionLag::topic, PartitionLag::partition, PartitionLag::committedOffset,
                PartitionLag::endOffset, PartitionLag::lag, PartitionLag::memberId);
    }
}

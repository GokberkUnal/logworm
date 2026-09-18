package com.gokgor.logworm.shell;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

import org.jline.terminal.Terminal;
import org.springframework.shell.core.command.annotation.Command;
import org.springframework.shell.core.command.annotation.Option;

import com.gokgor.logworm.consumergroup.ConsumerGroupService;
import com.gokgor.logworm.consumergroup.ConsumerGroupSummary;
import com.gokgor.logworm.consumergroup.PartitionLag;
import com.gokgor.logworm.shell.Rule.Color;

import lombok.RequiredArgsConstructor;

@InteractiveShellComponent
@RequiredArgsConstructor
public class GroupCommands {

    private static final DateTimeFormatter CLOCK = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final String CLEAR_SCREEN = "[H[2J";

    private final ConsumerGroupService consumerGroupService;
    private final Tables tables;
    private final ShellSession session;
    private final Terminal terminal;

    @Command(name = "groups", group = "Consumer groups", description = "List consumer groups with total lag (colored by the lag thresholds)")
    public String groups() {
        return groupsTable(session.lagWatch(), false);
    }

    @Command(name = "group", group = "Consumer groups", description = "Members and per-partition lag of one group")
    public String group(@Option(longName = "id", required = true, description = "Group id") String id) {
        LagWatch watch = session.lagWatch();
        var detail = consumerGroupService.getGroup(id);
        var header = detail.groupId()
                + "  state=" + detail.state()
                + "  members=" + detail.members().size()
                + "  lag=" + detail.totalLag()
                + (detail.coordinator() != null ? "  coordinator=" + detail.coordinator().id() : "") + "\n";
        return header + tables.renderStyled(detail.partitions(),
                new String[] {"topic", "partition", "committed", "end", "lag", "member"},
                p -> style(watch.colorFor(p.lag())),
                PartitionLag::topic, PartitionLag::partition, PartitionLag::committedOffset,
                PartitionLag::endOffset, PartitionLag::lag, PartitionLag::memberId);
    }

    @Command(name = "lag", group = "Consumer groups", description = "Show or change lag monitoring: --groups a,b (or 'all'), --warn N, --critical N")
    public String lag(
            @Option(longName = "groups", description = "Comma-separated group ids, or 'all'") String groups,
            @Option(longName = "warn", description = "Lag from which rows turn yellow") Long warn,
            @Option(longName = "critical", description = "Lag from which rows turn red") Long critical) {
        LagWatch w = session.lagWatch();
        List<String> ids = groups == null ? w.groups() : parseGroups(groups);
        long warnLag = warn != null ? warn : w.warnLag();
        long critLag = critical != null ? critical : Math.max(warnLag, w.critLag());
        session.setLagWatch(new LagWatch(ids, warnLag, critLag));
        return "Lag watch: " + session.lagWatch().describe();
    }

    @Command(name = "watch", group = "Consumer groups", description = "Live group/lag table, refreshed every N seconds; Ctrl+C stops it")
    public String watch(
            @Option(longName = "interval", description = "Seconds between refreshes (default 2)") Integer interval,
            @Option(longName = "count", description = "How many refreshes (default 10; 0 = until Ctrl+C)") Integer count,
            @Option(longName = "id", description = "Watch one group's partitions instead of the group list") String id)
            throws InterruptedException {
        long millis = Math.max(200, (interval != null ? interval : 2) * 1000L);
        int max = count != null ? count : 10; // default finite; --count 0 loops until Ctrl+C
        LagWatch watch = session.lagWatch();
        var out = terminal.writer();
        int refreshes = 0;
        try {
            while (max == 0 || refreshes < max) {
                String body;
                try {
                    body = id == null ? groupsTable(watch, true) : group(id);
                } catch (RuntimeException e) {
                    body = "Error: " + e.getMessage();
                }
                out.print(CLEAR_SCREEN);
                refreshes++;
                String progress = max > 0 ? "  [" + refreshes + "/" + max + "]" : "";
                out.println(LocalTime.now().format(CLOCK) + "  every " + (millis / 1000.0) + "s  ("
                        + watch.describe() + ")" + progress + (max == 0 ? "  Ctrl+C to stop" : ""));
                out.println(body);
                out.flush();
                if (max == 0 || refreshes < max) {
                    Thread.sleep(millis);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return "Watch stopped after " + refreshes + " refresh(es).";
    }

    private String groupsTable(LagWatch watch, boolean onlyWatched) {
        List<ConsumerGroupSummary> groups = consumerGroupService.listGroups().stream()
                .filter(g -> !onlyWatched || watch.includes(g.groupId()))
                .toList();
        if (groups.isEmpty()) {
            return onlyWatched && !watch.watchesAll()
                    ? "None of the watched groups exist: " + String.join(", ", watch.groups())
                    : "No consumer groups.";
        }
        return tables.renderStyled(groups,
                new String[] {"group", "state", "type", "members", "topics", "lag", "status"},
                g -> style(watch.colorFor(g.totalLag())),
                ConsumerGroupSummary::groupId, ConsumerGroupSummary::state, ConsumerGroupSummary::type,
                ConsumerGroupSummary::memberCount, g -> String.join(", ", g.topics()), ConsumerGroupSummary::totalLag,
                g -> statusOf(watch.colorFor(g.totalLag())));
    }

    static String statusOf(Color c) {
        return switch (c) {
            case RED -> "CRITICAL";
            case YELLOW -> "WARN";
            case GREEN -> "ok";
            default -> "";
        };
    }

    private static String style(Color c) {
        return c == Color.NONE || c == Color.GREEN ? null : c.ansi;
    }

    static List<String> parseGroups(String spec) {
        if (spec == null || spec.isBlank() || spec.trim().equalsIgnoreCase("all")) {
            return List.of();
        }
        return Arrays.stream(spec.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
    }
}

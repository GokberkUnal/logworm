package com.gokgor.logworm.shell;

import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.gokgor.logworm.consumergroup.ConsumerGroupService;
import com.gokgor.logworm.consumergroup.ConsumerGroupSummary;

import lombok.RequiredArgsConstructor;

/**
 * Wizard step 5: consumer-group monitoring. Which groups to watch and from which lag a row
 * turns yellow (warning) or red (critical).
 */
@InteractiveShellComponent
@RequiredArgsConstructor
public class ConsumerGroupStep {

    static final String SKIP = "Skip  (no group monitoring)";
    static final String ALL = "All groups";
    static final String PICK = "Pick groups...";

    private final Choices choices;
    private final ShellSession session;
    private final ConsumerGroupService consumerGroupService;

    public void ask() {
        ask(System.out);
    }

    void ask(PrintStream out) {
        List<ConsumerGroupSummary> groups;
        try {
            groups = consumerGroupService.listGroups();
        } catch (RuntimeException e) {
            out.println("Could not list consumer groups (" + e.getMessage() + "); set up monitoring later with 'lag'.");
            return;
        }
        if (groups.isEmpty()) {
            out.println("No consumer groups on this cluster yet; 'watch' will pick them up once they appear.");
            return;
        }

        Map<String, String> options = new LinkedHashMap<>();
        options.put(SKIP, SKIP);
        options.put(ALL + "  (" + groups.size() + ")", ALL);
        options.put(PICK, PICK);
        String picked = choices.select("Monitor consumer groups?", options);
        if (SKIP.equals(picked)) {
            return;
        }

        List<String> chosen = List.of();
        if (PICK.equals(picked)) {
            List<String> labels = groups.stream().map(ConsumerGroupStep::label).toList();
            List<String> selected = choices.selectMany("Which groups? (space to toggle, enter to confirm)", labels);
            chosen = selected.stream().map(l -> l.substring(0, l.indexOf("  ("))).toList();
            if (chosen.isEmpty()) {
                out.println("Nothing picked; watching all groups.");
            }
        }

        long warn = askThreshold("Warning lag (yellow from)", LagWatch.DEFAULT_WARN);
        long crit = askThreshold("Critical lag (red from)", Math.max(warn, LagWatch.DEFAULT_CRIT));
        if (crit < warn) {
            out.println("Critical must be >= warning; using " + warn + " for both.");
            crit = warn;
        }
        LagWatch watch = new LagWatch(chosen, warn, crit);
        session.setLagWatch(watch);
        out.println("Lag watch: " + watch.describe());
    }

    private long askThreshold(String question, long defaultValue) {
        while (true) {
            String typed = choices.text(question, String.valueOf(defaultValue)).trim();
            try {
                long v = Long.parseLong(typed);
                if (v >= 0) {
                    return v;
                }
            } catch (NumberFormatException ignored) {
                // fall through
            }
            System.out.println("Please enter a non-negative number.");
        }
    }

    static String label(ConsumerGroupSummary g) {
        return g.groupId() + "  (" + g.state() + ", " + g.memberCount() + " member" + (g.memberCount() == 1 ? "" : "s")
                + ", lag " + g.totalLag() + ")";
    }
}

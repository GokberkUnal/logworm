package com.gokgor.logworm.shell;

import java.util.List;

import com.gokgor.logworm.shell.Rule.Color;

/**
 * Consumer-group monitoring settings: which groups (empty = all) and the lag thresholds.
 *
 * @param groups  group ids to watch; empty means every group on the cluster
 * @param warnLag lag at or above which a row turns yellow
 * @param critLag lag at or above which a row turns red
 */
public record LagWatch(List<String> groups, long warnLag, long critLag) {

    static final String SESSION_KEY = "lagWatch";
    static final long DEFAULT_WARN = 100;
    static final long DEFAULT_CRIT = 1000;

    public LagWatch {
        if (warnLag < 0 || critLag < 0) {
            throw new IllegalArgumentException("thresholds must be >= 0");
        }
        if (critLag < warnLag) {
            throw new IllegalArgumentException("critical threshold must be >= warning threshold");
        }
        groups = List.copyOf(groups);
    }

    public static LagWatch defaults() {
        return new LagWatch(List.of(), DEFAULT_WARN, DEFAULT_CRIT);
    }

    public boolean watchesAll() {
        return groups.isEmpty();
    }

    public boolean includes(String groupId) {
        return groups.isEmpty() || groups.contains(groupId);
    }

    /** Color for a lag value; NONE below the warning threshold. Null lag (no commit) is NONE. */
    public Color colorFor(Long lag) {
        if (lag == null) {
            return Color.NONE;
        }
        if (lag >= critLag) {
            return Color.RED;
        }
        if (lag >= warnLag) {
            return Color.YELLOW;
        }
        return Color.GREEN;
    }

    public String describe() {
        return (watchesAll() ? "all groups" : String.join(", ", groups))
                + "  warn ≥ " + warnLag + "  critical ≥ " + critLag;
    }
}

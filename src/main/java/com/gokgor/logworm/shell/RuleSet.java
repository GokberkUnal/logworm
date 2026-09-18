package com.gokgor.logworm.shell;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.gokgor.logworm.message.KafkaMessage;

/**
 * Filters (all must match for a message to be shown) and alerts (first match colors/labels the row).
 */
public record RuleSet(List<Rule> filters, List<Rule> alerts) {

    static final String SESSION_KEY = "rules";

    public static RuleSet empty() {
        return new RuleSet(List.of(), List.of());
    }

    public boolean isEmpty() {
        return filters.isEmpty() && alerts.isEmpty();
    }

    public boolean passes(KafkaMessage m) {
        return filters.stream().allMatch(r -> r.matches(m));
    }

    public Optional<Rule> alertFor(KafkaMessage m) {
        return alerts.stream().filter(r -> r.matches(m)).findFirst();
    }

    public RuleSet plusFilter(Rule r) {
        List<Rule> f = new ArrayList<>(filters);
        f.add(r);
        return new RuleSet(List.copyOf(f), alerts);
    }

    public RuleSet plusAlert(Rule r) {
        List<Rule> a = new ArrayList<>(alerts);
        a.add(r);
        return new RuleSet(filters, List.copyOf(a));
    }

    public String describe() {
        if (isEmpty()) {
            return "no filters, no alerts";
        }
        var sb = new StringBuilder();
        if (!filters.isEmpty()) {
            sb.append("filters: ");
            for (int i = 0; i < filters.size(); i++) {
                sb.append(i > 0 ? " AND " : "").append(filters.get(i).describe());
            }
        }
        if (!alerts.isEmpty()) {
            sb.append(filters.isEmpty() ? "" : "; ").append("alerts: ");
            for (int i = 0; i < alerts.size(); i++) {
                Rule a = alerts.get(i);
                sb.append(i > 0 ? ", " : "").append(a.color().name().toLowerCase()).append(" when ").append(a.describe());
            }
        }
        return sb.toString();
    }
}

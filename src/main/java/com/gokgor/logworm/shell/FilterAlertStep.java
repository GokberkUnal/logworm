package com.gokgor.logworm.shell;

import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.gokgor.logworm.shell.Rule.Color;
import com.gokgor.logworm.shell.Rule.Condition;

import lombok.RequiredArgsConstructor;

/**
 * Wizard step 4: filters (which messages to show) and alerts (color + label for matches).
 * Both are rules on one field: the record key or a JSON field discovered from recent messages.
 */
@InteractiveShellComponent
@RequiredArgsConstructor
public class FilterAlertStep {

    static final String NO_FILTER = "Show everything";
    static final String ADD_FILTER = "Only messages where a field matches...";
    static final String NO_ALERTS = "No alerts";
    static final String ADD_ALERT = "Add an alert rule (color + label when a field matches)...";
    static final String OTHER_FIELD = "Other field...";

    private final Choices choices;
    private final ShellSession session;
    private final FieldSampler sampler;

    public void ask() {
        ask(System.out);
    }

    void ask(PrintStream out) {
        String topic = session.currentTopic().orElse(null);
        if (topic == null) {
            return;
        }
        List<String> fields;
        try {
            fields = sampler.sampleWithKey(topic);
        } catch (RuntimeException e) {
            out.println("Could not sample fields (" + e.getMessage() + "); rules can be added later with 'filter' / 'alert'.");
            fields = List.of(Rule.KEY_FIELD);
        }

        RuleSet rules = session.rules();

        Map<String, String> f = new LinkedHashMap<>();
        f.put(NO_FILTER, NO_FILTER);
        f.put(ADD_FILTER, ADD_FILTER);
        if (ADD_FILTER.equals(choices.select("Filter messages?", f))) {
            do {
                rules = rules.plusFilter(askRule(fields, Color.NONE));
            } while (choices.confirm("Add another filter?", false));
        }

        Map<String, String> a = new LinkedHashMap<>();
        a.put(NO_ALERTS, NO_ALERTS);
        a.put(ADD_ALERT, ADD_ALERT);
        if (ADD_ALERT.equals(choices.select("Alerts?", a))) {
            do {
                Color color = Color.valueOf(choices.select("Color for matches",
                        List.of(Color.RED.name(), Color.YELLOW.name(), Color.GREEN.name(), Color.CYAN.name())));
                Rule rule = askRule(fields, color);
                String label = choices.text("Label", rule.describe());
                rules = rules.plusAlert(rule.withLabel(label));
            } while (choices.confirm("Add another alert?", false));
        }

        session.setRules(rules);
        out.println("Rules: " + rules.describe());
    }

    private Rule askRule(List<String> fields, Color color) {
        Map<String, String> options = new LinkedHashMap<>();
        fields.forEach(x -> options.put(x, x));
        options.put(OTHER_FIELD, OTHER_FIELD);
        String field = choices.select("Which field?", options);
        if (OTHER_FIELD.equals(field)) {
            field = choices.text("Field path (dotted, e.g. payload.status)", "").trim();
        }

        Map<String, String> conds = new LinkedHashMap<>();
        // single-token labels: the selector's type-to-filter does not cope with spaces
        conds.put("is-null  (missing or null)", Condition.IS_NULL.name());
        conds.put("not-null", Condition.IS_NOT_NULL.name());
        conds.put("equals", Condition.EQUALS.name());
        conds.put("not-equals", Condition.NOT_EQUALS.name());
        conds.put("contains", Condition.CONTAINS.name());
        conds.put("greater-than", Condition.GREATER_THAN.name());
        conds.put("less-than", Condition.LESS_THAN.name());
        Condition condition = Condition.valueOf(choices.select("Condition for '" + field + "'", conds));

        String value = null;
        if (condition.needsValue()) {
            value = choices.text("Value", "");
        }
        Rule r = new Rule(field, condition, value, color, null);
        return r.withLabel(r.describe());
    }
}

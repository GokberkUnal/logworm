package com.gokgor.logworm.shell;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gokgor.logworm.message.KafkaMessage;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

/** Turns messages into table rows / single lines according to the selected fields. */
@InteractiveShellComponent
public class MessageFormatter {

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault());
    static final String[] META = {"part", "offset", "time", "key"};

    private final ObjectMapper json;

    public MessageFormatter(ObjectMapper json) {
        this.json = json;
    }

    public String[] headers(ViewSettings view) {
        return headers(view, false);
    }

    /** @param withAlert adds a trailing "alert" column for the matching rule's label */
    public String[] headers(ViewSettings view, boolean withAlert) {
        List<String> h = new ArrayList<>(List.of(META));
        if (view.hasFields()) {
            h.addAll(view.fields());
        } else {
            h.add("value");
        }
        if (withAlert) {
            h.add("alert");
        }
        return h.toArray(String[]::new);
    }

    public Object[] row(KafkaMessage m, ViewSettings view, int valueWidth) {
        return row(m, view, valueWidth, null);
    }

    public Object[] row(KafkaMessage m, ViewSettings view, int valueWidth, RuleSet rules) {
        List<Object> cells = new ArrayList<>();
        cells.add(m.partition());
        cells.add(m.offset());
        cells.add(TIME.format(m.timestamp()));
        cells.add(m.key() == null ? "" : m.key());
        if (view.hasFields()) {
            for (String f : view.fields()) {
                cells.add(field(m, f));
            }
        } else {
            cells.add(truncate(valueText(m), valueWidth));
        }
        if (rules != null && !rules.alerts().isEmpty()) {
            cells.add(rules.alertFor(m).map(Rule::label).orElse(""));
        }
        return cells.toArray();
    }

    /** {@link #line} wrapped in the matching alert's color and prefixed with its label. */
    public String line(KafkaMessage m, ViewSettings view, int width, RuleSet rules) {
        var alert = rules == null ? java.util.Optional.<Rule>empty() : rules.alertFor(m);
        if (alert.isEmpty()) {
            return line(m, view, width);
        }
        Rule r = alert.get();
        String prefix = "[" + r.label() + "] ";
        return r.color().ansi + prefix + line(m, view, Math.max(10, width - prefix.length())) + Rule.Color.RESET;
    }

    /** One line for live tail: {@code p1@1203 12:04:33.209 key  a=1 b=2} or the value. */
    public String line(KafkaMessage m, ViewSettings view, int width) {
        var sb = new StringBuilder()
                .append('p').append(m.partition()).append('@').append(m.offset()).append(' ')
                .append(TIME.format(m.timestamp())).append(' ')
                .append(m.key() == null ? "-" : m.key()).append("  ");
        if (view.hasFields()) {
            for (String f : view.fields()) {
                sb.append(f).append('=').append(field(m, f)).append("  ");
            }
        } else {
            sb.append(valueText(m));
        }
        return truncate(sb.toString().stripTrailing(), width);
    }

    /**
     * One block per message for humans:
     * <pre>
     * 14:03:44.614  key=order-service  p2@1014
     *   level    INFO --> ERROR
     *   message  Order created
     * </pre>
     * Fields are the selected ones, otherwise every (flattened) JSON field of the message, or
     * {@code value} for non-JSON. A field that differs from the previous message with the same key
     * is printed as {@code previous --> current}. An alert colors the whole block and adds its label.
     */
    public String pretty(KafkaMessage m, ViewSettings view, RuleSet rules, ChangeTracker tracker) {
        var alert = rules == null ? java.util.Optional.<Rule>empty() : rules.alertFor(m);
        var sb = new StringBuilder();
        sb.append(TIME.format(m.timestamp())).append("  key=").append(m.key() == null ? "-" : m.key())
                .append("  p").append(m.partition()).append('@').append(m.offset());
        alert.ifPresent(r -> sb.append("  [").append(r.label()).append(']'));
        List<ChangeTracker.Change> changes = tracker.track(m.key(), fieldValues(m, view));
        int nameWidth = changes.stream().mapToInt(c -> c.field().length()).max().orElse(0);
        for (var c : changes) {
            sb.append("\n  ").append(c.field()).append(" ".repeat(nameWidth - c.field().length() + 2));
            if (c.changed()) {
                sb.append(c.previous()).append(" --> ");
            }
            sb.append(c.current());
        }
        String text = sb.toString();
        return alert.map(r -> r.color().ansi + text + Rule.Color.RESET).orElse(text);
    }

    /** Field name → display value: the selected fields, else all flattened JSON fields, else {@code value}. */
    static Map<String, String> fieldValues(KafkaMessage m, ViewSettings view) {
        Map<String, String> values = new LinkedHashMap<>();
        if (view.hasFields()) {
            for (String f : view.fields()) {
                String v = field(m, f);
                values.put(f, v.isEmpty() ? "-" : v);
            }
        } else if (m.value() instanceof JsonNode node && node.isObject()) {
            Set<String> keys = new LinkedHashSet<>();
            collect("", node, keys, 0);
            for (String k : keys) {
                String v = field(m, k);
                values.put(k, v.isEmpty() ? "-" : v);
            }
        } else {
            values.put("value", valueText(m));
        }
        return values;
    }

    /**
     * One compact JSON object per message: partition, offset, timestamp, key, then either the
     * whole {@code value} or one property per selected field; {@code alert} carries the matching
     * rule's label. Colored like {@link #line(KafkaMessage, ViewSettings, int, RuleSet)}.
     */
    public String json(KafkaMessage m, ViewSettings view, RuleSet rules) {
        ObjectNode o = json.createObjectNode();
        o.put("partition", m.partition());
        o.put("offset", m.offset());
        o.put("timestamp", m.timestamp().toString());
        o.put("key", m.key());
        if (view.hasFields()) {
            ObjectNode fields = o.putObject("fields");
            for (String f : view.fields()) {
                fields.set(f, fieldNode(m, f));
            }
        } else if (m.value() instanceof JsonNode node) {
            o.set("value", node);
        } else {
            o.put("value", m.value() == null ? null : m.value().toString());
        }
        var alert = rules == null ? java.util.Optional.<Rule>empty() : rules.alertFor(m);
        alert.ifPresent(r -> o.put("alert", r.label()));
        String text = o.toString();
        return alert.map(r -> r.color().ansi + text + Rule.Color.RESET).orElse(text);
    }

    private JsonNode fieldNode(KafkaMessage m, String path) {
        if (!(m.value() instanceof JsonNode node)) {
            return json.getNodeFactory().nullNode();
        }
        JsonNode at = node.at("/" + path.replace('.', '/'));
        return at.isMissingNode() ? json.getNodeFactory().nullNode() : at;
    }

    /** Value of a dotted path inside a JSON value; empty string when absent or the value is not JSON. */
    public static String field(KafkaMessage m, String path) {
        if (!(m.value() instanceof JsonNode node)) {
            return "";
        }
        JsonNode at = node.at("/" + path.replace('.', '/'));
        if (at.isMissingNode() || at.isNull()) {
            return "";
        }
        return at.isValueNode() ? at.asString() : at.toString();
    }

    /** Union of top-level JSON keys across messages, in first-seen order; nested objects become {@code a.b}. */
    public static List<String> discoverFields(List<KafkaMessage> messages) {
        Set<String> keys = new LinkedHashSet<>();
        for (KafkaMessage m : messages) {
            if (m.value() instanceof JsonNode node && node.isObject()) {
                collect("", node, keys, 0);
            }
        }
        return List.copyOf(keys);
    }

    private static void collect(String prefix, JsonNode node, Set<String> keys, int depth) {
        for (var e : node.properties()) {
            String path = prefix.isEmpty() ? e.getKey() : prefix + "." + e.getKey();
            if (e.getValue().isObject() && depth < 2) {
                collect(path, e.getValue(), keys, depth + 1);
            } else {
                keys.add(path);
            }
        }
    }

    static String valueText(KafkaMessage m) {
        return m.value() == null ? "<null>" : m.value().toString();
    }

    static String truncate(String s, int width) {
        if (width <= 0 || s.length() <= width) {
            return s;
        }
        return s.substring(0, Math.max(0, width - 1)) + "…";
    }
}

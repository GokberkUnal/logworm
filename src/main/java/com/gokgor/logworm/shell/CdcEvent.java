package com.gokgor.logworm.shell;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.gokgor.logworm.message.KafkaMessage;

import tools.jackson.databind.JsonNode;

/**
 * A change-data-capture event in the Debezium envelope: {@code op}, {@code before}, {@code after},
 * {@code source}. The {@code {"schema":…, "payload":…}} wrapper of the JSON converter is unwrapped.
 *
 * @param before row before the change; null when absent (inserts, or no before image)
 * @param after  row after the change; null when absent (deletes)
 * @param table  {@code db.table} / {@code schema.table} from {@code source}, or null
 */
public record CdcEvent(Op op, String table, Map<String, String> before, Map<String, String> after) {

    public enum Op {
        INSERT("c"), UPDATE("u"), DELETE("d"), SNAPSHOT("r"), TRUNCATE("t");

        final String code;

        Op(String code) {
            this.code = code;
        }

        static Optional<Op> of(String code) {
            for (Op op : values()) {
                if (op.code.equals(code)) {
                    return Optional.of(op);
                }
            }
            return Optional.empty();
        }

        Rule.Color color() {
            return switch (this) {
                case INSERT -> Rule.Color.GREEN;
                case UPDATE -> Rule.Color.YELLOW;
                case DELETE, TRUNCATE -> Rule.Color.RED;
                case SNAPSHOT -> Rule.Color.CYAN;
            };
        }
    }

    /** One column of an update: {@code before} / {@code after} are null when the column is missing on that side. */
    public record ColumnChange(String column, String before, String after) {
    }

    public static Optional<CdcEvent> parse(KafkaMessage m) {
        if (!(m.value() instanceof JsonNode node) || !node.isObject()) {
            return Optional.empty();
        }
        if (node.has("schema") && node.path("payload").isObject()) {
            node = node.get("payload");
        }
        Optional<Op> op = Op.of(node.path("op").asString(""));
        if (op.isEmpty() || (op.get() != Op.TRUNCATE && !node.has("before") && !node.has("after"))) {
            return Optional.empty();
        }
        return Optional.of(new CdcEvent(op.get(), table(node.path("source")), row(node.get("before")), row(node.get("after"))));
    }

    public static boolean isCdc(KafkaMessage m) {
        return parse(m).isPresent();
    }

    /** True when most of the JSON messages in the sample are CDC events. */
    public static boolean looksLikeCdc(List<KafkaMessage> sample) {
        long json = sample.stream().filter(m -> m.value() instanceof JsonNode).count();
        long cdc = sample.stream().filter(CdcEvent::isCdc).count();
        return json > 0 && cdc * 2 > json;
    }

    /** Columns whose value differs between before and after (added / removed columns included), in after's order. */
    public List<ColumnChange> changes() {
        if (before == null || after == null) {
            return List.of();
        }
        Map<String, ColumnChange> out = new LinkedHashMap<>();
        after.forEach((col, a) -> {
            String b = before.get(col);
            if (!before.containsKey(col) || !b.equals(a)) {
                out.put(col, new ColumnChange(col, before.containsKey(col) ? b : null, a));
            }
        });
        before.forEach((col, b) -> {
            if (!after.containsKey(col)) {
                out.put(col, new ColumnChange(col, b, null));
            }
        });
        return List.copyOf(out.values());
    }

    private static String table(JsonNode source) {
        if (!source.isObject() || !source.path("table").isString()) {
            return null;
        }
        // Postgres has db + schema + table; MySQL has db + table
        String container = source.path("schema").isString() ? source.get("schema").asString()
                : source.path("db").asString(null);
        String table = source.get("table").asString();
        return container != null ? container + "." + table : table;
    }

    private static Map<String, String> row(JsonNode node) {
        if (node == null || !node.isObject()) {
            return null;
        }
        Map<String, String> row = new LinkedHashMap<>();
        for (var e : node.properties()) {
            JsonNode v = e.getValue();
            row.put(e.getKey(), v.isNull() ? "null" : v.isValueNode() ? v.asString() : v.toString());
        }
        return row;
    }
}

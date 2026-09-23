package com.gokgor.logworm.shell;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import com.gokgor.logworm.message.KafkaMessage;

import tools.jackson.databind.JsonNode;

/**
 * A condition on one field of a message. Used both as a filter (show only matches) and as an
 * alert (color + label matches).
 *
 * @param field {@code key} for the record key, otherwise a dotted JSON path into the value
 * @param value comparison operand; unused for the null checks
 */
public record Rule(String field, Condition condition, String value, Color color, String label) {

    public static final String KEY_FIELD = "key";

    public enum Condition {
        IS_NULL, IS_NOT_NULL, EQUALS, NOT_EQUALS, IN, NOT_IN, CONTAINS, GREATER_THAN, LESS_THAN;

        public boolean needsValue() {
            return this != IS_NULL && this != IS_NOT_NULL;
        }
    }

    public enum Color {
        RED("[31m"), YELLOW("[33m"), GREEN("[32m"), CYAN("[36m"), NONE("");

        public static final String RESET = "[0m";
        public final String ansi;

        Color(String ansi) {
            this.ansi = ansi;
        }
    }

    public static Rule filter(String field, Condition condition, String value) {
        return new Rule(field, condition, value, Color.NONE, null);
    }

    public boolean matches(KafkaMessage m) {
        String actual = valueOf(m, field);
        return switch (condition) {
            case IS_NULL -> actual == null;
            case IS_NOT_NULL -> actual != null;
            case EQUALS -> actual != null && actual.equals(value);
            case NOT_EQUALS -> actual == null || !actual.equals(value);
            case IN -> actual != null && values().contains(actual);
            case NOT_IN -> actual == null || !values().contains(actual);
            case CONTAINS -> actual != null && actual.contains(value);
            case GREATER_THAN -> compare(actual, value) > 0;
            case LESS_THAN -> compare(actual, value) < 0;
        };
    }

    /** The comma-separated operand of {@link Condition#IN} / {@link Condition#NOT_IN}, trimmed. */
    public List<String> values() {
        if (value == null) {
            return List.of();
        }
        return Arrays.stream(value.split(",")).map(String::trim).filter(v -> !v.isEmpty()).toList();
    }

    /** Field text, or null when the key/field is absent, JSON null, or the value is not JSON. */
    static String valueOf(KafkaMessage m, String field) {
        if (KEY_FIELD.equals(field)) {
            return m.key();
        }
        if (!(m.value() instanceof JsonNode node)) {
            return null;
        }
        JsonNode at = node.at("/" + field.replace('.', '/'));
        if (at.isMissingNode() || at.isNull()) {
            return null;
        }
        return at.isValueNode() ? at.asString() : at.toString();
    }

    private static int compare(String actual, String expected) {
        if (actual == null) {
            return 0; // neither greater nor less
        }
        try {
            return Double.compare(Double.parseDouble(actual), Double.parseDouble(expected));
        } catch (NumberFormatException e) {
            return actual.compareTo(expected);
        }
    }

    /** Human form: {@code price is null}, {@code level == ERROR}. */
    public String describe() {
        String op = switch (condition) {
            case IS_NULL -> "is null";
            case IS_NOT_NULL -> "is not null";
            case EQUALS -> "== " + value;
            case NOT_EQUALS -> "!= " + value;
            case IN -> "in (" + String.join(", ", values()) + ")";
            case NOT_IN -> "not in (" + String.join(", ", values()) + ")";
            case CONTAINS -> "contains " + value;
            case GREATER_THAN -> "> " + value;
            case LESS_THAN -> "< " + value;
        };
        return field + " " + op;
    }

    /**
     * Parses the {@code --when} syntax of the shell commands:
     * {@code null}, {@code notnull}, {@code eq:X}, {@code ne:X}, {@code in:A,B}, {@code notin:A,B}, {@code contains:X}, {@code gt:N}, {@code lt:N}.
     */
    public static Rule parse(String field, String when, Color color, String label) {
        if (field == null || field.isBlank()) {
            throw new IllegalArgumentException("--field is required");
        }
        if (when == null || when.isBlank()) {
            throw new IllegalArgumentException("--when is required (null, notnull, eq:X, ne:X, in:A,B, notin:A,B, contains:X, gt:N, lt:N)");
        }
        String w = when.trim();
        int colon = w.indexOf(':');
        String op = (colon < 0 ? w : w.substring(0, colon)).toLowerCase(Locale.ROOT);
        String operand = colon < 0 ? null : w.substring(colon + 1);
        Condition c = switch (op) {
            case "null" -> Condition.IS_NULL;
            case "notnull" -> Condition.IS_NOT_NULL;
            case "eq" -> Condition.EQUALS;
            case "ne" -> Condition.NOT_EQUALS;
            case "in" -> Condition.IN;
            case "notin", "nin" -> Condition.NOT_IN;
            case "contains" -> Condition.CONTAINS;
            case "gt" -> Condition.GREATER_THAN;
            case "lt" -> Condition.LESS_THAN;
            default -> throw new IllegalArgumentException("Unknown condition '" + op + "'");
        };
        if (c.needsValue() && (operand == null || operand.isEmpty())) {
            throw new IllegalArgumentException("'" + op + "' needs a value, e.g. " + op + ":ERROR");
        }
        Rule r = new Rule(field.trim(), c, operand, color != null ? color : Color.NONE, label);
        return label == null || label.isBlank() ? r.withLabel(r.describe()) : r;
    }

    public Rule withLabel(String newLabel) {
        return new Rule(field, condition, value, color, newLabel);
    }
}

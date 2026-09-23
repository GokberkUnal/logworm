package com.gokgor.logworm.shell;

/** How {@code show} and {@code tail} print messages. */
public enum OutputFormat {
    /** One readable block per message: time, key, then one line per field with {@code previous --> current} on change. */
    PRETTY,
    /** Box-drawn table with one row per message ({@code tail} falls back to {@link #LINE}). */
    TABLE,
    /** One JSON object per line (partition, offset, timestamp, key, value or the selected fields, alert). */
    JSON,
    /** One compact text line per message: {@code p1@1203 12:04:33.209 key  value}. */
    LINE;

    static final String SESSION_KEY = "format";

    public static OutputFormat parse(String s) {
        try {
            return valueOf(s.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown format '" + s + "'; use pretty, table, json or line");
        }
    }

    @Override
    public String toString() {
        return name().toLowerCase();
    }
}

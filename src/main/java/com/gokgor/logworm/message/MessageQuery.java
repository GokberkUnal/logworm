package com.gokgor.logworm.message;

/**
 * Validated read request for a topic.
 *
 * <p>Two modes:
 * <ul>
 *   <li><b>tail</b> ({@code offset == null}): the newest {@code limit} records, across all
 *       partitions or the one given.</li>
 *   <li><b>range</b> ({@code offset != null}): {@code limit} records forward from that offset;
 *       requires {@code partition}.</li>
 * </ul>
 *
 * @param key    substring the key must contain, or null
 * @param value  substring the raw value must contain, or null
 * @param header {@code name} (header present) or {@code name=value} (exact value), or null
 */
public record MessageQuery(
        Integer partition,
        Long offset,
        int limit,
        String key,
        String value,
        String header,
        ValueFormat format) {

    public static final int DEFAULT_LIMIT = 100;
    public static final int MAX_LIMIT = 1000;
    /** When filtering, read up to this many times {@code limit} to find enough matches. */
    public static final int FILTER_SCAN_FACTOR = 10;
    public static final int MAX_SCAN = 5000;

    public MessageQuery {
        if (limit < 1 || limit > MAX_LIMIT) {
            throw new InvalidQueryException("limit must be between 1 and " + MAX_LIMIT);
        }
        if (offset != null && offset < 0) {
            throw new InvalidQueryException("offset must be >= 0");
        }
        if (offset != null && partition == null) {
            throw new InvalidQueryException("offset requires a partition");
        }
        if (partition != null && partition < 0) {
            throw new InvalidQueryException("partition must be >= 0");
        }
        if (header != null && header.isBlank()) {
            throw new InvalidQueryException("header filter must not be blank");
        }
        key = blankToNull(key);
        value = blankToNull(value);
        header = blankToNull(header);
        format = format != null ? format : ValueFormat.AUTO;
    }

    public boolean isTail() {
        return offset == null;
    }

    public boolean hasFilters() {
        return key != null || value != null || header != null;
    }

    /** Records to read from Kafka before filtering. */
    public int scanWindow() {
        return hasFilters() ? Math.min(limit * FILTER_SCAN_FACTOR, MAX_SCAN) : limit;
    }

    private static String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s;
    }
}

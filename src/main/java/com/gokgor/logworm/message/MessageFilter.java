package com.gokgor.logworm.message;

import java.util.function.Predicate;

/** Builds the record predicate for a query's key / value / header filters. */
public final class MessageFilter {

    private MessageFilter() {
    }

    public static Predicate<RawMessage> from(MessageQuery query) {
        return from(query.key(), query.value(), query.header());
    }

    /** Each argument may be null (no constraint); substrings for key/value, {@code name} or {@code name=value} for header. */
    public static Predicate<RawMessage> from(String key, String value, String header) {
        Predicate<RawMessage> p = m -> true;
        if (key != null) {
            p = p.and(m -> m.key() != null && m.key().contains(key));
        }
        if (value != null) {
            p = p.and(m -> m.value() != null && m.value().contains(value));
        }
        if (header != null) {
            p = p.and(headerPredicate(header));
        }
        return p;
    }

    private static Predicate<RawMessage> headerPredicate(String spec) {
        int eq = spec.indexOf('=');
        if (eq < 0) {
            return m -> m.headers().stream().anyMatch(h -> h.key().equals(spec));
        }
        String name = spec.substring(0, eq);
        String value = spec.substring(eq + 1);
        return m -> m.headers().stream()
                .anyMatch(h -> h.key().equals(name) && value.equals(h.value()));
    }
}

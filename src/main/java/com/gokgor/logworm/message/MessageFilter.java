package com.gokgor.logworm.message;

import java.util.function.Predicate;

/** Builds the record predicate for a query's key / value / header filters. */
final class MessageFilter {

    private MessageFilter() {
    }

    static Predicate<RawMessage> from(MessageQuery query) {
        Predicate<RawMessage> p = m -> true;
        if (query.key() != null) {
            p = p.and(m -> m.key() != null && m.key().contains(query.key()));
        }
        if (query.value() != null) {
            p = p.and(m -> m.value() != null && m.value().contains(query.value()));
        }
        if (query.header() != null) {
            p = p.and(headerPredicate(query.header()));
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

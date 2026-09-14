package com.gokgor.logworm.stream;

import java.util.function.Predicate;

import com.gokgor.logworm.message.InvalidQueryException;
import com.gokgor.logworm.message.MessageFilter;
import com.gokgor.logworm.message.RawMessage;
import com.gokgor.logworm.message.ValueFormat;

/**
 * Validated live-tail request.
 *
 * @param rate maximum messages per second delivered to this client; excess records are dropped and reported
 */
public record StreamQuery(
        Integer partition,
        String key,
        String value,
        String header,
        ValueFormat format,
        int rate) {

    public StreamQuery {
        if (partition != null && partition < 0) {
            throw new InvalidQueryException("partition must be >= 0");
        }
        if (rate < 1) {
            throw new InvalidQueryException("rate must be >= 1");
        }
        if (header != null && header.isBlank()) {
            throw new InvalidQueryException("header filter must not be blank");
        }
        key = blankToNull(key);
        value = blankToNull(value);
        header = blankToNull(header);
        format = format != null ? format : ValueFormat.AUTO;
    }

    public Predicate<RawMessage> filter() {
        return MessageFilter.from(key, value, header);
    }

    private static String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s;
    }
}
